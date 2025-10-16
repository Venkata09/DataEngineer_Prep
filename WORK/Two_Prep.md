No worries—let’s make this super simple. You’re on **Windows + IntelliJ + Docker**. Just pick **one** of the two approaches below.

---

# Approach 1 (Do this first): Fix it at the source

Make your files **LF** so Docker won’t choke.

### Step 1 — IntelliJ

* **File → Settings → Editor → Code Style → Line separator → “Unix and macOS (\n)”**
* (Bottom-right of any file, you should see **LF**. If it shows **CRLF**, click it → switch to **LF**.)

### Step 2 — Add `.editorconfig` (at repo root)

```
# .editorconfig
root = true

[*]
end_of_line = lf
insert_final_newline = true
charset = utf-8
trim_trailing_whitespace = true
```

### Step 3 — Add `.gitattributes` (enforce LF in Git)

```
# .gitattributes
*         text=auto eol=lf
*.sh      text eol=lf
Dockerfile text eol=lf
*.yml     text eol=lf
*.yaml    text eol=lf
*.env     text eol=lf

# If you have Windows-only scripts, keep CRLF for them:
*.bat     text eol=crlf
*.cmd     text eol=crlf
```

### Step 4 — Tell Git to stop CRLF shenanigans (Windows)

Run in your repo (PowerShell or Git Bash):

```bash
git config core.autocrlf false
git config core.eol lf
git add .gitattributes
git rm --cached -r .
git reset --hard
```

> That normalizes existing files to LF based on `.gitattributes`.

---

# Approach 2 (Safety net): Fix it during Docker build

If a stray CRLF still sneaks in, make Docker convert it automatically.
**Put this *after* your `COPY` in the Dockerfile.**

### If your base image is Debian/Ubuntu:

```Dockerfile
RUN apt-get update && apt-get install -y dos2unix \
 && find /app -type f -name "*.sh" -exec dos2unix {} \; \
 && chmod +x /app/*.sh || true
```

### If your base image is Alpine:

```Dockerfile
RUN apk add --no-cache dos2unix \
 && find /app -type f -name "*.sh" -exec dos2unix {} \; \
 && chmod +x /app/*.sh || true
```

### If you can’t install packages:

```Dockerfile
RUN find /app -type f -name "*.sh" -exec sh -c 'sed -i "s/\r$//" "$1" && chmod +x "$1"' _ {} \;
```

---

# One-time “fix everything now” (Windows PowerShell)

If you want to convert the whole project to LF immediately:

```powershell
Get-ChildItem -Recurse -File -Include *.sh,*.yml,*.yaml,*.env,Dockerfile |
  ForEach-Object {
    $content = Get-Content $_.FullName -Raw
    ($content -replace "`r`n","`n") | Set-Content -NoNewline -Encoding utf8 $_.FullName
  }
```

---

## Which should I choose?

* **Most people:** Do **Approach 1** (IntelliJ + Git + attributes).
* **Extra safe:** Keep **Approach 2** in your Dockerfile as a guardrail.

If you paste your Dockerfile `FROM …` line (Alpine vs Debian/Ubuntu), I’ll drop in the exact 3-line fix in the right spot.
