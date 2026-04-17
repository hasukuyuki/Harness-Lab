#!/bin/bash
#
# Harness Lab Uninstall Script
#
# Safely removes Harness Lab from the system.
#
# Usage:
#   curl -sSL https://raw.githubusercontent.com/3452808350-max/Harness-Lab/main/scripts/uninstall.sh | bash
#   ./uninstall.sh [--keep-data] [--keep-config] [--force]
#
# Options:
#   --keep-data      Keep PostgreSQL database and Redis data
#   --keep-config    Keep .env configuration file
#   --force          Skip confirmation prompt (dangerous!)
#   --help           Show this help
#
# Warning: This will permanently remove Harness Lab!
#          Use --keep-data and --keep-config to preserve important data.

set -e

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m'

log_info() { echo -e "${BLUE}[INFO]${NC} $1"; }
log_ok() { echo -e "${GREEN}[OK]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_err() { echo -e "${RED}[ERR]${NC} $1"; }

# Defaults
KEEP_DATA=false
KEEP_CONFIG=false
FORCE=false
INSTALL_DIR="${HOME}/.harness-lab"
BACKUP_DIR="${HOME}/.harness-lab-backup-$(date +%Y%m%d%H%M%S)"

# Parse args
while [[ $# -gt 0 ]]; do
    case $1 in
        --keep-data)
            KEEP_DATA=true
            shift
            ;;
        --keep-config)
            KEEP_CONFIG=true
            shift
            ;;
        --force)
            FORCE=true
            shift
            ;;
        --help)
            head -20 "$0" | tail -15
            exit 0
            ;;
        *)
            log_err "Unknown option: $1"
            exit 1
            ;;
    esac
done

# Warning banner
show_warning() {
    echo ""
    echo -e "${RED}╔════════════════════════════════════════════════════════╗${NC}"
    echo -e "${RED}║              WARNING: UNINSTALL                        ║${NC}"
    echo -e "${RED}╚════════════════════════════════════════════════════════╝${NC}"
    echo ""
    echo -e "${YELLOW}This will permanently remove Harness Lab from your system.${NC}"
    echo ""
    echo "  Files:     $INSTALL_DIR"
    echo "  Service:   harness-lab.service"
    echo ""
    
    if [ "$KEEP_DATA" = true ]; then
        echo -e "${GREEN}  Database:  KEEP (--keep-data)${NC}"
    else
        echo -e "${RED}  Database:  DELETE${NC}"
    fi
    
    if [ "$KEEP_CONFIG" = true ]; then
        echo -e "${GREEN}  Config:    KEEP (--keep-config)${NC}"
    else
        echo -e "${RED}  Config:    DELETE${NC}"
    fi
    
    echo ""
}

# Confirmation prompt
confirm() {
    if [ "$FORCE" = true ]; then
        log_warn "Force mode - skipping confirmation"
        return 0
    fi
    
    echo -e "${YELLOW}Type 'yes' to confirm uninstall:${NC}"
    read -r CONFIRM
    
    if [ "$CONFIRM" != "yes" ]; then
        log_info "Uninstall cancelled"
        exit 0
    fi
}

# Check installation exists
check_install() {
    if [ ! -d "$INSTALL_DIR" ]; then
        log_warn "Harness Lab not found at $INSTALL_DIR"
        log_info "Nothing to uninstall"
        exit 0
    fi
}

# Create backup before deletion
create_backup() {
    log_info "Creating backup at $BACKUP_DIR..."
    
    mkdir -p "$BACKUP_DIR"
    
    # Backup config
    if [ -f "$INSTALL_DIR/.env" ]; then
        cp "$INSTALL_DIR/.env" "$BACKUP_DIR/.env"
        log_ok "Config backed up"
    fi
    
    # Backup database dump (if not keeping)
    if [ "$KEEP_DATA" = false ]; then
        if command -v pg_dump &> /dev/null; then
            log_info "Dumping database..."
            sudo -u postgres pg_dump harness_lab > "$BACKUP_DIR/database.sql" 2>&1 || \
                log_warn "Could not dump database (may not exist)"
        fi
    fi
    
    log_ok "Backup created at $BACKUP_DIR"
}

# Stop service
stop_service() {
    log_info "Stopping service..."
    
    if systemctl list-unit-files | grep -q "harness-lab.service"; then
        sudo systemctl stop harness-lab 2>&1 || true
        log_ok "Service stopped"
    else
        log_info "No systemd service found"
    fi
}

# Remove systemd service
remove_service() {
    log_info "Removing systemd service..."
    
    if systemctl list-unit-files | grep -q "harness-lab.service"; then
        sudo systemctl disable harness-lab 2>&1 || true
        sudo rm -f /etc/systemd/system/harness-lab.service
        sudo systemctl daemon-reload
        log_ok "Service removed"
    fi
}

# Remove database
remove_database() {
    if [ "$KEEP_DATA" = true ]; then
        log_info "Keeping database (--keep-data)"
        return 0
    fi
    
    log_info "Removing database..."
    
    if command -v psql &> /dev/null; then
        # Drop database
        sudo -u postgres psql -c "DROP DATABASE IF EXISTS harness_lab;" 2>&1 || \
            log_warn "Could not drop database"
        
        # Drop user (optional)
        # sudo -u postgres psql -c "DROP USER IF EXISTS harness;" 2>&1 || true
        
        log_ok "Database removed"
    else
        log_warn "PostgreSQL not found - cannot remove database"
    fi
}

# Remove Redis data
remove_redis() {
    if [ "$KEEP_DATA" = true ]; then
        log_info "Keeping Redis data (--keep-data)"
        return 0
    fi
    
    log_info "Clearing Redis data..."
    
    if command -v redis-cli &> /dev/null; then
        redis-cli FLUSHDB 2>&1 || log_warn "Could not clear Redis"
        log_ok "Redis data cleared"
    else
        log_warn "Redis not found"
    fi
}

# Remove Docker containers/images (optional)
remove_docker() {
    log_info "Cleaning Docker resources..."
    
    if command -v docker &> /dev/null; then
        # Remove Harness Lab sandbox containers
        docker ps -a --filter "name=harness" -q | xargs -r docker rm -f 2>&1 || true
        
        # Remove sandbox image
        docker rmi harness-lab/sandbox:local 2>&1 || true
        
        log_ok "Docker resources cleaned"
    fi
}

# Remove files
remove_files() {
    log_info "Removing files..."
    
    # Keep config if requested
    if [ "$KEEP_CONFIG" = true ] && [ -f "$INSTALL_DIR/.env" ]; then
        mkdir -p "$BACKUP_DIR"
        cp "$INSTALL_DIR/.env" "$BACKUP_DIR/.env.saved"
        log_ok "Config saved to $BACKUP_DIR/.env.saved"
    fi
    
    # Remove installation directory
    rm -rf "$INSTALL_DIR"
    log_ok "Files removed"
}

# Print summary
print_summary() {
    echo ""
    echo -e "${GREEN}╔════════════════════════════════════════════════════════╗${NC}"
    echo -e "${GREEN}║              Uninstall Complete                        ║${NC}"
    echo -e "${GREEN}╚════════════════════════════════════════════════════════╝${NC}"
    echo ""
    echo "  Removed:"
    echo "    - Files: $INSTALL_DIR"
    echo "    - Service: harness-lab.service"
    
    if [ "$KEEP_DATA" = false ]; then
        echo "    - Database: harness_lab"
        echo "    - Redis: FLUSHDB"
    fi
    
    echo ""
    echo "  Backup saved to: $BACKUP_DIR"
    
    if [ "$KEEP_CONFIG" = true ]; then
        echo "  Config saved to: $BACKUP_DIR/.env.saved"
    fi
    
    if [ "$KEEP_DATA" = false ] && [ -f "$BACKUP_DIR/database.sql" ]; then
        echo "  Database dump: $BACKUP_DIR/database.sql"
    fi
    
    echo ""
    echo -e "${YELLOW}Note: PostgreSQL and Redis services are NOT removed.${NC}"
    echo "      To remove them completely:"
    echo "        sudo apt remove postgresql-16 redis-server docker-ce"
    echo ""
}

# Main flow
main() {
    show_warning
    confirm
    check_install
    create_backup
    stop_service
    remove_service
    remove_database
    remove_redis
    remove_docker
    remove_files
    print_summary
    
    log_ok "=== Uninstall Complete ==="
}

main