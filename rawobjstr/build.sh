#!/usr/bin/env bash
#
# build.sh - Build rawobjstr (rawobjstr CLI)
#
# Usage:
#   ./build.sh --location /path/to/build/dir
#
# The build directory holds all compiled artifacts. To clean up:
#   rm -rf /path/to/build/dir
#
set -euo pipefail

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BOLD='\033[1m'
NC='\033[0m'

usage() {
    echo "Usage: $0 --location <build-directory>"
    echo ""
    echo "  --location <dir>   Directory for build output (will be created if needed)"
    echo "  --release          Build in release mode (default)"
    echo "  --debug            Build in debug mode"
    echo "  --help             Show this help"
    exit 1
}

BUILD_DIR=""
PROFILE="release"
CARGO_PROFILE_FLAG="--release"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --location)
            BUILD_DIR="$2"
            shift 2
            ;;
        --release)
            PROFILE="release"
            CARGO_PROFILE_FLAG="--release"
            shift
            ;;
        --debug)
            PROFILE="debug"
            CARGO_PROFILE_FLAG=""
            shift
            ;;
        --help|-h)
            usage
            ;;
        *)
            echo -e "${RED}Unknown option: $1${NC}"
            usage
            ;;
    esac
done

if [[ -z "$BUILD_DIR" ]]; then
    echo -e "${RED}Error: --location is required${NC}"
    echo ""
    usage
fi

# Resolve to absolute path
BUILD_DIR="$(cd "$(dirname "$BUILD_DIR")" 2>/dev/null && pwd)/$(basename "$BUILD_DIR")" || BUILD_DIR="$(pwd)/$BUILD_DIR"

# Find the source directory (where this script lives)
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

echo -e "${BOLD}rawobjstr build${NC}"
echo "=============================="
echo "Source:    $SCRIPT_DIR"
echo "Build to:  $BUILD_DIR"
echo "Profile:   $PROFILE"
echo ""

# ---- Check prerequisites ----

MISSING=""

# Check for cargo/rustc
if ! command -v cargo >/dev/null 2>&1; then
    echo -e "${RED}ERROR: cargo not found${NC}"
    echo ""
    echo "Rust and Cargo are required to build this project."
    echo ""
    echo "Option 1 - Install via rustup (recommended):"
    echo "  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh"
    echo "  source \$HOME/.cargo/env"
    echo ""
    echo "Option 2 - Install via apt (Debian/Ubuntu):"
    echo "  sudo apt update && sudo apt install -y cargo"
    echo ""
    echo "Then re-run this script."
    exit 1
fi

RUST_VER="$(rustc --version 2>/dev/null || echo 'unknown')"
CARGO_VER="$(cargo --version 2>/dev/null || echo 'unknown')"
echo -e "Rust:   ${GREEN}$RUST_VER${NC}"
echo -e "Cargo:  ${GREEN}$CARGO_VER${NC}"

# Check for a C linker (gcc or cc)
if ! command -v cc >/dev/null 2>&1 && ! command -v gcc >/dev/null 2>&1; then
    MISSING="$MISSING cc/gcc"
fi

if [[ -n "$MISSING" ]]; then
    echo ""
    echo -e "${RED}ERROR: Missing system packages:${NC} $MISSING"
    echo ""
    echo "On Debian/Ubuntu:"
    echo "  sudo apt update && sudo apt install -y build-essential"
    echo ""
    echo "On Fedora/RHEL:"
    echo "  sudo dnf install -y gcc"
    echo ""
    exit 1
fi

echo ""

# ---- Create build directory ----

mkdir -p "$BUILD_DIR"

# ---- Build the CLI tool (rawobjstr) ----

echo -e "${BOLD}Building rawobjstr CLI...${NC}"
CARGO_TARGET_DIR="$BUILD_DIR" cargo build $CARGO_PROFILE_FLAG \
    --manifest-path "$SCRIPT_DIR/Cargo.toml" \
    --bin rawobjstr

CLI_BIN="$BUILD_DIR/$PROFILE/rawobjstr"
if [[ -f "$CLI_BIN" ]]; then
    echo -e "  ${GREEN}OK${NC} $CLI_BIN"
else
    echo -e "  ${RED}FAILED${NC} - binary not found at $CLI_BIN"
    exit 1
fi

# ---- Done ----

echo ""
echo "=============================="
echo -e "${GREEN}Build complete!${NC}"
echo ""
echo "Binary:"
echo "  CLI: $CLI_BIN"
echo ""
echo -e "${BOLD}Quick start:${NC}"
echo ""
echo "  $CLI_BIN format --file /tmp/store.raw --size 268435456"
echo "  $CLI_BIN put --file /tmp/store.raw --key hello.txt --from hello.txt"
echo "  $CLI_BIN list --file /tmp/store.raw"
echo ""
echo "To clean up all build artifacts:"
echo "  rm -rf $BUILD_DIR"
echo ""
echo -e "${YELLOW}If you like this project, please give it a star!${NC}"
echo "  <REPO_URL>"
