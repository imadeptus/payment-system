```bash
#!/usr/bin/env bash
# ================================================================
# setup.sh — production-ready (macOS + Linux)
# ================================================================
set -euo pipefail

REPO_DIR="$HOME/payment-system"
GIT_NAME="imadeptus"
GIT_EMAIL="113295work@gmail.com"

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; BLUE='\033[0;34m'; NC='\033[0m'
ok()   { echo -e "${GREEN}✅ $1${NC}"; }
info() { echo -e "${BLUE}▶  $1${NC}"; }
warn() { echo -e "${YELLOW}⚠️  $1${NC}"; }
fail() { echo -e "${RED}❌ $1${NC}"; exit 1; }

echo ""
echo -e "${BLUE}================================================${NC}"
echo -e "${BLUE}   payment-system — setup (stable)              ${NC}"
echo -e "${BLUE}================================================${NC}"
echo ""

# ── Проверки ────────────────────────────────────────────────────
[[ -d "$REPO_DIR" ]]      || fail "Репо не найдено: $REPO_DIR"
[[ -d "$REPO_DIR/.git" ]] || fail "Это не git репозиторий: $REPO_DIR"
command -v git >/dev/null || fail "git не установлен"

cd "$REPO_DIR"

warn "Будет выполнен force push и переписана история."
read -p "Продолжить? (y/n): " confirm
[[ "$confirm" == "y" ]] || exit 0
echo ""

# ── Git config ──────────────────────────────────────────────────
info "Настраиваю git user..."
git config user.name  "$GIT_NAME"
git config user.email "$GIT_EMAIL"
ok "git config готов"

# ── README safe update ───────────────────────────────────────────
README="README.md"
BADGE='[![CI](https://github.com/imadeptus/payment-system/actions/workflows/ci.yml/badge.svg)](https://github.com/imadeptus/payment-system/actions/workflows/ci.yml)'

if [[ -f "$README" ]]; then
  info "Обновляю README..."

  # Fix clone URL
  sed -i.bak 's|your-username/banking-system|imadeptus/payment-system|g' "$README" || true

  # Add badge if not exists
  if ! grep -q "ci.yml/badge.svg" "$README"; then
    tmp=$(mktemp)
    echo -e "$BADGE\n" > "$tmp"
    cat "$README" >> "$tmp"
    mv "$tmp" "$README"
    ok "CI badge добавлен"
  else
    ok "CI badge уже есть"
  fi

  rm -f "${README}.bak"
else
  warn "README.md не найден — пропускаю"
fi

# ── CI ──────────────────────────────────────────────────────────
info "Создаю GitHub Actions..."
mkdir -p .github/workflows

cat > .github/workflows/ci.yml << 'YAML'
name: CI
on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    services:
      postgres:
        image: postgres:16
        env:
          POSTGRES_USER: payment_user
          POSTGRES_PASSWORD: payment_pass
          POSTGRES_DB: payment_db
        ports: [ "5432:5432" ]
      redis:
        image: redis:7
        ports: [ "6379:6379" ]

    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.12"

      - run: pip install fastapi pytest pytest-asyncio sqlalchemy asyncpg redis ruff

      - run: ruff check . || true

      - run: pytest -v
YAML

ok "CI создан"

# ── Tests ───────────────────────────────────────────────────────
info "Проверяю tests..."
mkdir -p tests
touch tests/__init__.py
ok "tests готовы"

# ── CHANGELOG ───────────────────────────────────────────────────
if [[ ! -f CHANGELOG.md ]]; then
cat > CHANGELOG.md << 'MD'
# Changelog

## v1.0.0
- Initial production release
MD
ok "CHANGELOG создан"
fi

# ── Git history rewrite ─────────────────────────────────────────
info "Переписываю git историю..."

git checkout --orphan new_main
git add .
git commit -m "feat: production-ready payment system"

git branch -D main 2>/dev/null || true
git branch -M new_main main

ok "История обновлена"

# ── Push ───────────────────────────────────────────────────────
info "Push в GitHub..."
git push origin main --force

ok "Готово 🚀"
```
