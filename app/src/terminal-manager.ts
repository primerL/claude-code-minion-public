import { spawn } from 'child_process';
import { v4 as uuidv4 } from 'uuid';
import { TerminalInstance } from './types.js';
import * as path from 'path';
import * as fs from 'fs';

// Dynamic import for strip-ansi (ESM module)
let stripAnsi: (text: string) => string;

async function initStripAnsi() {
  const module = await import('strip-ansi');
  stripAnsi = module.default;
}

initStripAnsi();

interface QueuedMessage {
  input: string;
  resolve: (success: boolean) => void;
  oauthToken?: string;  // Store token at queue time so it's used when processed
}

export class TerminalManager {
  private terminals: Map<string, TerminalInstance> = new Map();
  private outputBuffers: Map<string, string[]> = new Map();
  private workingDirectory: string;
  private appDirectory: string;
  private shellPath: string;
  private claudePath: string;
  private nodePath: string;
  private mcpConfigs: Map<string, string> = new Map(); // channelId -> mcpConfigPath
  private sessionIds: Map<string, string> = new Map(); // channelId -> claude session UUID
  private sessionIdsFilePath: string; // Path to persist session IDs
  private busyChannels: Set<string> = new Set(); // channels with running claude commands
  private messageQueues: Map<string, QueuedMessage[]> = new Map(); // channelId -> queued messages
  private awaitingSessionId: Set<string> = new Set(); // channels waiting to capture session_id from JSON output
  private latestUsageStats: Map<string, any> = new Map(); // channelId -> latest usage stats from JSON output
  private onQueueProcessCallback?: (channelId: string, message: string) => Promise<void>;
  private onAgentTurnCompleteCallback?: (channelId: string) => Promise<void>;

  constructor(
    workingDirectory: string,
    appDirectory: string,
    onQueueProcess?: (channelId: string, message: string) => Promise<void>,
    onAgentTurnComplete?: (channelId: string) => Promise<void>
  ) {
    this.workingDirectory = workingDirectory;
    this.appDirectory = appDirectory;
    this.shellPath = this.resolveShellPath();
    this.claudePath = this.resolveExecutablePath(
      'claude',
      [
        path.join(process.env.HOME || '', '.local', 'bin', 'claude'),
        '/usr/local/bin/claude',
        '/opt/homebrew/bin/claude',
      ]
    );
    this.nodePath = process.execPath;
    this.sessionIdsFilePath = path.join(workingDirectory, '.minion-claude-sessions.json');
    this.onQueueProcessCallback = onQueueProcess;
    this.onAgentTurnCompleteCallback = onAgentTurnComplete;
    console.log(`[TerminalManager] shell=${this.shellPath} claude=${this.claudePath} node=${this.nodePath}`);
    this.loadSessionIds();
  }

  private isExecutable(filePath: string): boolean {
    if (!filePath) {
      return false;
    }

    try {
      fs.accessSync(filePath, fs.constants.X_OK);
      return true;
    } catch {
      return false;
    }
  }

  private resolveExecutablePath(command: string, fallbacks: string[] = []): string {
    const pathEntries = (process.env.PATH || '').split(path.delimiter).filter(Boolean);

    for (const candidate of [...fallbacks, ...pathEntries.map(entry => path.join(entry, command))]) {
      if (this.isExecutable(candidate)) {
        return candidate;
      }
    }

    return command;
  }

  private resolveShellPath(): string {
    if (process.platform === 'win32') {
      return 'powershell.exe';
    }

    const preferredShell = process.env.SHELL;
    if (preferredShell && this.isExecutable(preferredShell)) {
      return preferredShell;
    }

    return this.resolveExecutablePath('bash', ['/bin/bash', '/bin/zsh']);
  }

  // Load persisted session IDs from disk
  private loadSessionIds(): void {
    try {
      if (fs.existsSync(this.sessionIdsFilePath)) {
        const data = JSON.parse(fs.readFileSync(this.sessionIdsFilePath, 'utf-8'));
        for (const [channelId, sessionId] of Object.entries(data)) {
          this.sessionIds.set(channelId, sessionId as string);
        }
        console.log(`[SessionIds] Loaded ${this.sessionIds.size} persisted Claude session IDs`);
      }
    } catch (error) {
      console.error('[SessionIds] Error loading session IDs:', error);
    }
  }

  // Save session IDs to disk
  private saveSessionIds(): void {
    try {
      const data: Record<string, string> = {};
      this.sessionIds.forEach((v, k) => data[k] = v);
      fs.writeFileSync(this.sessionIdsFilePath, JSON.stringify(data, null, 2));
    } catch (error) {
      console.error('[SessionIds] Error saving session IDs:', error);
    }
  }

  async spawnClaudeCode(channelId: string, mcpPort: number, oauthToken?: string): Promise<TerminalInstance> {
    const id = uuidv4();

    // Create MCP config for this instance (stored in app directory)
    const mcpConfigDir = path.join(this.appDirectory, '.claude-minion', channelId);
    fs.mkdirSync(mcpConfigDir, { recursive: true });

    const mcpConfigPath = path.join(mcpConfigDir, 'mcp-config.json');
    const mcpConfig = {
      mcpServers: {
        'slack-messenger': {
          command: this.nodePath,
          args: [path.join(this.appDirectory, 'dist', 'mcp-server.js')],
          env: {
            MCP_PORT: mcpPort.toString(),
            CHANNEL_ID: channelId,
            ORCHESTRATOR_URL: `http://localhost:${process.env.ORCHESTRATOR_PORT || 3000}`,
          },
        },
      },
    };
    fs.writeFileSync(mcpConfigPath, JSON.stringify(mcpConfig, null, 2));

    // Store MCP config path for this channel
    this.mcpConfigs.set(channelId, mcpConfigPath);
    // Session ID will be captured from first command's JSON output

    const terminal: TerminalInstance = {
      id,
      channelId,
      activeProcess: undefined,
      mcpPort,
      lastActivity: new Date(),
    };

    this.terminals.set(id, terminal);
    this.outputBuffers.set(id, []);

    return terminal;
  }

  private buildCommandEnv(channelId: string, mcpPort: number, oauthToken?: string): NodeJS.ProcessEnv {
    const commandEnv: NodeJS.ProcessEnv = {
      ...process.env,
      TERM: 'xterm-256color',
      MCP_PORT: mcpPort.toString(),
      CHANNEL_ID: channelId,
      PATH: Array.from(new Set([
        path.dirname(this.claudePath),
        path.dirname(this.nodePath),
        ...(process.env.PATH || '').split(path.delimiter).filter(Boolean),
      ])).join(path.delimiter),
    };

    if (oauthToken) {
      commandEnv.CLAUDE_CODE_OAUTH_TOKEN = oauthToken;
      console.log(`[Terminal ${channelId}] Using specific OAuth token (${oauthToken.substring(0, 15)}...)`);
    }

    return commandEnv;
  }

  private appendOutput(terminalId: string, channelId: string, data: string): void {
    const buffer = this.outputBuffers.get(terminalId);
    if (buffer) {
      buffer.push(data);
      if (buffer.length > 1000) {
        buffer.shift();
      }
    }

    const terminal = this.terminals.get(terminalId);
    if (terminal) {
      terminal.lastActivity = new Date();
    }

    const cleanData = stripAnsi ? stripAnsi(data) : data;
    if (cleanData.trim()) {
      console.log(`[Terminal ${channelId}] ${cleanData}`);
    }

    if (this.awaitingSessionId.has(channelId)) {
      const sessionMatch = cleanData.match(/"session_id"\s*:\s*"([^"]+)"/);
      if (sessionMatch) {
        const extractedSessionId = sessionMatch[1];
        this.sessionIds.set(channelId, extractedSessionId);
        this.awaitingSessionId.delete(channelId);
        this.saveSessionIds();
        console.log(`[Session] Captured session ID for channel ${channelId}: ${extractedSessionId.substring(0, 8)}...`);
      }
    }

    const resultStart = cleanData.indexOf('{"type":"result"');
    if (resultStart !== -1) {
      let braceCount = 0;
      let endIdx = -1;
      for (let i = resultStart; i < cleanData.length; i++) {
        if (cleanData[i] === '{') braceCount++;
        if (cleanData[i] === '}') braceCount--;
        if (braceCount === 0) {
          endIdx = i + 1;
          break;
        }
      }
      if (endIdx !== -1) {
        try {
          const resultJson = JSON.parse(cleanData.substring(resultStart, endIdx));
          if (resultJson.usage) {
            this.latestUsageStats.set(channelId, resultJson.usage);
            console.log(`[Usage] Captured usage stats for channel ${channelId}`);
          }
        } catch {
          // Ignore partial JSON chunks.
        }
      }
    }
  }

  private handleProcessExit(channelId: string, terminalId: string, exitCode: number | null, signal: NodeJS.Signals | null): void {
    const terminal = this.terminals.get(terminalId);
    if (terminal) {
      terminal.activeProcess = undefined;
      terminal.lastActivity = new Date();
    }

    if (this.busyChannels.has(channelId)) {
      this.busyChannels.delete(channelId);
      console.log(`[Terminal ${channelId}] Claude command finished (code=${exitCode ?? 'null'}, signal=${signal ?? 'none'})`);

      if (this.onAgentTurnCompleteCallback) {
        this.onAgentTurnCompleteCallback(channelId).catch(err => {
          console.error('[AgentTurnComplete] Error in callback:', err);
        });
      }

      this.processQueue(channelId);
    }
  }

  private spawnClaudeProcess(
    terminal: TerminalInstance,
    args: string[],
    oauthToken?: string,
    onStdout?: (data: string) => void,
    onStderr?: (data: string) => void,
    onExit?: (exitCode: number | null, signal: NodeJS.Signals | null) => void,
  ) {
    const commandEnv = this.buildCommandEnv(terminal.channelId, terminal.mcpPort, oauthToken);
    const child = spawn(this.claudePath, args, {
      cwd: this.workingDirectory,
      env: commandEnv,
      stdio: ['ignore', 'pipe', 'pipe'],
    });

    terminal.activeProcess = child;
    terminal.lastActivity = new Date();

    child.stdout.on('data', (chunk: Buffer | string) => {
      const data = chunk.toString();
      this.appendOutput(terminal.id, terminal.channelId, data);
      onStdout?.(data);
    });

    child.stderr.on('data', (chunk: Buffer | string) => {
      const data = chunk.toString();
      this.appendOutput(terminal.id, terminal.channelId, data);
      onStderr?.(data);
    });

    child.on('error', (error) => {
      console.error(`[Terminal ${terminal.channelId}] Failed to spawn Claude command:`, error);
      this.appendOutput(terminal.id, terminal.channelId, `Failed to spawn Claude command: ${error.message}\n`);
      this.handleProcessExit(terminal.channelId, terminal.id, null, null);
      onExit?.(null, null);
    });

    child.on('close', (exitCode, signal) => {
      this.handleProcessExit(terminal.channelId, terminal.id, exitCode, signal);
      onExit?.(exitCode, signal);
    });

    return child;
  }

  // Queue a message to be sent to Claude (handles busy state)
  // oauthToken: Optional OAuth token to use for this command (enables dynamic token switching)
  async sendInput(terminalId: string, input: string, oauthToken?: string): Promise<boolean> {
    const terminal = this.terminals.get(terminalId);
    if (!terminal) {
      console.error(`Terminal ${terminalId} not found`);
      return false;
    }

    const channelId = terminal.channelId;

    // If channel is busy, queue the message
    if (this.busyChannels.has(channelId)) {
      console.log(`[Queue] Channel ${channelId} is busy, queuing message`);
      return new Promise((resolve) => {
        const queue = this.messageQueues.get(channelId) || [];
        queue.push({ input, resolve, oauthToken });  // Store token with queued message
        this.messageQueues.set(channelId, queue);
      });
    }

    // Send immediately
    return this.sendInputNow(terminalId, input, oauthToken);
  }

  // Actually send input to Claude (internal method)
  // oauthToken: If provided, this token is used for this specific command (dynamic token switching)
  private sendInputNow(terminalId: string, input: string, oauthToken?: string): boolean {
    const terminal = this.terminals.get(terminalId);
    if (!terminal) {
      console.error(`Terminal ${terminalId} not found`);
      return false;
    }

    const channelId = terminal.channelId;
    const mcpConfigPath = this.mcpConfigs.get(channelId);
    if (!mcpConfigPath) {
      console.error(`MCP config not found for channel ${channelId}`);
      return false;
    }

    // Mark channel as busy
    this.busyChannels.add(channelId);

    // Build the claude command - always use --output-format json to capture usage stats
    const existingSessionId = this.sessionIds.get(channelId);

    // Get model from environment variable, default to sonnet-4.5
    const model = process.env.CLAUDE_MODEL || 'claude-sonnet-4-5-20250929';

    const args = ['-p', input.replace(/\r/g, ''), '--model', model, '--output-format', 'json'];
    if (existingSessionId) {
      args.push('--resume', existingSessionId);
      const tokenLog = oauthToken ? ` (token: ${oauthToken.substring(0, 15)}...)` : '';
      console.log(`[Sending to Claude] ${this.claudePath} -p "..." --model ${model} --output-format json --resume "${existingSessionId.substring(0, 8)}..."${tokenLog}`);
    } else {
      this.awaitingSessionId.add(channelId);
      const tokenLog = oauthToken ? ` (token: ${oauthToken.substring(0, 15)}...)` : '';
      console.log(`[Sending to Claude] ${this.claudePath} -p "..." --model ${model} --output-format json (new conversation)${tokenLog}`);
    }
    args.push('--mcp-config', mcpConfigPath);

    this.spawnClaudeProcess(terminal, args, oauthToken);
    return true;
  }

  // Process next message in the queue for a channel
  private processQueue(channelId: string): void {
    const queue = this.messageQueues.get(channelId);
    if (!queue || queue.length === 0) {
      return;
    }

    const terminal = this.getTerminalByChannelId(channelId);
    if (!terminal) {
      // Clear queue if terminal is gone
      this.messageQueues.delete(channelId);
      return;
    }

    const next = queue.shift()!;
    console.log(`[Queue] Processing next message for channel ${channelId}`);

    // Notify via callback that we're processing this message
    if (this.onQueueProcessCallback) {
      this.onQueueProcessCallback(channelId, next.input).catch(err => {
        console.error('[Queue] Error in callback:', err);
      });
    }

    // Pass the stored OAuth token (from when message was queued)
    const success = this.sendInputNow(terminal.id, next.input, next.oauthToken);
    next.resolve(success);
  }

  // Check if channel is currently processing a command
  isChannelBusy(channelId: string): boolean {
    return this.busyChannels.has(channelId);
  }

  // Get queue length for a channel
  getQueueLength(channelId: string): number {
    return this.messageQueues.get(channelId)?.length || 0;
  }

  sendRawInput(terminalId: string, input: string): boolean {
    console.warn(`sendRawInput is not supported without PTY. Ignored input for terminal ${terminalId}: ${input.slice(0, 40)}`);
    return true;
  }

  // Send interrupt (Ctrl+C) to stop current claude command
  sendInterrupt(terminalId: string): boolean {
    const terminal = this.terminals.get(terminalId);
    if (!terminal) {
      console.error(`Terminal ${terminalId} not found`);
      return false;
    }

    if (!terminal.activeProcess) {
      console.error(`Terminal ${terminalId} has no active Claude process to interrupt`);
      return false;
    }

    terminal.activeProcess.kill('SIGINT');
    terminal.lastActivity = new Date();
    return true;
  }

  getOutput(terminalId: string, lines: number = 50): string[] {
    const buffer = this.outputBuffers.get(terminalId);
    if (!buffer) {
      return [];
    }

    const result = buffer.slice(-lines);
    // Strip ANSI codes for cleaner output
    return result.map(line => stripAnsi ? stripAnsi(line) : line);
  }

  getTerminal(terminalId: string): TerminalInstance | undefined {
    return this.terminals.get(terminalId);
  }

  getTerminalByChannelId(channelId: string): TerminalInstance | undefined {
    for (const terminal of this.terminals.values()) {
      if (terminal.channelId === channelId) {
        return terminal;
      }
    }
    return undefined;
  }

  killTerminal(terminalId: string): boolean {
    const terminal = this.terminals.get(terminalId);
    if (!terminal) {
      return false;
    }

    if (terminal.activeProcess) {
      terminal.activeProcess.kill('SIGTERM');
      terminal.activeProcess = undefined;
    }
    this.terminals.delete(terminalId);
    this.outputBuffers.delete(terminalId);
    return true;
  }

  killByChannelId(channelId: string): boolean {
    const terminal = this.getTerminalByChannelId(channelId);
    if (terminal) {
      return this.killTerminal(terminal.id);
    }
    return false;
  }

  getAllTerminals(): TerminalInstance[] {
    return Array.from(this.terminals.values());
  }

  resize(terminalId: string, cols: number, rows: number): boolean {
    const terminal = this.terminals.get(terminalId);
    if (!terminal) {
      return false;
    }

    void cols;
    void rows;
    return true;
  }

  // Reset conversation for a channel (clears session so next message starts fresh)
  resetConversation(channelId: string): void {
    // Clear session ID so next message will start a new conversation
    this.sessionIds.delete(channelId);
    this.awaitingSessionId.delete(channelId);
    this.saveSessionIds(); // Persist deletion to disk
    // Clear busy state and queue
    this.busyChannels.delete(channelId);
    this.messageQueues.delete(channelId);
    console.log(`[Reset] Cleared session for channel ${channelId}, next message will start new conversation`);
  }

  // Clear busy state for a channel (call after interrupt)
  clearBusyState(channelId: string): void {
    this.busyChannels.delete(channelId);
    // Also clear message queue since queued messages are likely no longer relevant
    const queueLength = this.messageQueues.get(channelId)?.length || 0;
    this.messageQueues.delete(channelId);
    console.log(`[Interrupt] Cleared busy state for channel ${channelId}, dropped ${queueLength} queued message(s)`);
  }

  // Get session ID for a channel
  getSessionId(channelId: string): string | undefined {
    return this.sessionIds.get(channelId);
  }

  // Get MCP config path for a channel
  getMcpConfigPath(channelId: string): string | undefined {
    return this.mcpConfigs.get(channelId);
  }

  // Get latest usage stats for a channel
  getLatestUsageStats(channelId: string): any | undefined {
    return this.latestUsageStats.get(channelId);
  }

  // Send input with JSON output format and return the parsed result
  async sendInputWithJsonOutput(terminalId: string, input: string): Promise<any> {
    const terminal = this.terminals.get(terminalId);
    if (!terminal) {
      throw new Error(`Terminal ${terminalId} not found`);
    }

    const channelId = terminal.channelId;
    const mcpConfigPath = this.mcpConfigs.get(channelId);
    if (!mcpConfigPath) {
      throw new Error(`MCP config not found for channel ${channelId}`);
    }

    const model = process.env.CLAUDE_MODEL || 'claude-sonnet-4-5-20250929';
    const existingSessionId = this.sessionIds.get(channelId);
    const args = ['-p', input.replace(/\r/g, ''), '--model', model, '--output-format', 'json'];
    if (existingSessionId) {
      args.push('--resume', existingSessionId);
    }
    args.push('--mcp-config', mcpConfigPath);

    console.log(`[/context] Sending: ${this.claudePath} -p "..." --model ${model} --output-format json${existingSessionId ? ` --resume "${existingSessionId.substring(0, 8)}..."` : ''}`);

    // Mark channel as busy
    this.busyChannels.add(channelId);

    return new Promise((resolve, reject) => {
      let output = '';
      let resolved = false;
      const timeout = setTimeout(() => {
        if (!resolved) {
          resolved = true;
          this.busyChannels.delete(channelId);
          reject(new Error('Timeout waiting for response'));
        }
      }, 60000); // 1 minute timeout

      const tryParseResult = () => {
        // Find the result JSON object - it starts with {"type":"result" and ends with }
        const startIdx = output.indexOf('{"type":"result"');
        if (startIdx === -1) return null;

        // Find the matching closing brace by counting braces
        let braceCount = 0;
        let endIdx = -1;
        for (let i = startIdx; i < output.length; i++) {
          if (output[i] === '{') braceCount++;
          if (output[i] === '}') braceCount--;
          if (braceCount === 0) {
            endIdx = i + 1;
            break;
          }
        }

        if (endIdx === -1) return null;

        try {
          const jsonStr = output.substring(startIdx, endIdx);
          const result = JSON.parse(jsonStr);
          return result;
        } catch (e) {
          // Not valid JSON yet
          return null;
        }
      };

      const dataHandler = (data: string) => {
        output += data;

        const result = tryParseResult();
        if (result && !resolved) {
          resolved = true;
          clearTimeout(timeout);
          this.busyChannels.delete(channelId);
          resolve(result);
        }
      };

      this.spawnClaudeProcess(
        terminal,
        args,
        undefined,
        dataHandler,
        dataHandler,
        () => {
          if (!resolved) {
            const result = tryParseResult();
            if (result) {
              resolved = true;
              clearTimeout(timeout);
              this.busyChannels.delete(channelId);
              resolve(result);
              return;
            }

            resolved = true;
            clearTimeout(timeout);
            this.busyChannels.delete(channelId);
            reject(new Error('Claude command exited before producing JSON output'));
          }
        }
      );
    });
  }
}
