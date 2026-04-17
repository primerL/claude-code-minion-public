#!/bin/bash

# One-click launcher for Claude Code Minion with pm2 auto-restart

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
APP_DIR="$SCRIPT_DIR/app"

cd "$APP_DIR" || exit 1

# Check if pm2 is installed globally or locally
if command -v pm2 &> /dev/null; then
    PM2="pm2"
elif [ -f "$APP_DIR/node_modules/.bin/pm2" ]; then
    PM2="$APP_DIR/node_modules/.bin/pm2"
else
    echo "Installing pm2..."
    npm install pm2 --save-dev
    PM2="$APP_DIR/node_modules/.bin/pm2"
fi

# Stop existing instance if running
$PM2 delete claude-minion 2>/dev/null || true

# Start with pm2
echo "Starting Claude Code Minion with pm2..."
$PM2 start npm --name "claude-minion" -- start \
    --time \
    --log "$SCRIPT_DIR/logs/minion.log" \
    --error "$SCRIPT_DIR/logs/minion-error.log"

# Create logs directory if it doesn't exist
mkdir -p "$SCRIPT_DIR/logs"

# Save pm2 process list (for auto-restart on system reboot)
$PM2 save

echo ""
echo "═══════════════════════════════════════════════════════════"
echo "  Claude Code Minion is running with auto-restart!"
echo "═══════════════════════════════════════════════════════════"
echo ""
echo "Useful commands:"
echo "  $PM2 logs claude-minion    # View logs"
echo "  $PM2 status                # Check status"
echo "  $PM2 restart claude-minion # Manual restart"
echo "  $PM2 stop claude-minion    # Stop the bot"
echo ""
echo "Logs are saved to: $SCRIPT_DIR/logs/"
echo ""

# Show initial logs
echo "Initial output:"
sleep 2
$PM2 logs claude-minion --lines 20 --nostream