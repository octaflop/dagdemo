# DagDemo — Windows Setup Instructions

This guide provides step-by-step instructions for setting up and running the DagDemo repository on Windows.

## Prerequisites

Before you begin, ensure you have the following installed:

1. **Python 3.11 or higher** - Download from [python.org](https://www.python.org/downloads/windows/)
   - During installation, check "Add Python to PATH"
2. **Git** - Download from [git-scm.com](https://git-scm.com/download/win)
3. **Command Prompt or PowerShell** - Built into Windows

## Installation Steps

### 1. Clone the Repository

Open Command Prompt or PowerShell and run:

```bash
git clone https://github.com/your-username/dagdemo.git
cd dagdemo
```

### 2. Install UV Package Manager

The project uses [uv](https://docs.astral.sh/uv/) for fast Python package management. Install it using:

```powershell
# PowerShell
iwr https://astral.sh/uv/install.ps1 -useb | iex

# Or in Command Prompt
curl -LsSf https://astral.sh/uv/install.sh | sh
```

After installation, restart your terminal to ensure `uv` is in your PATH.

### 3. Install Dependencies

```bash
uv sync
```

This creates a virtual environment and installs all required packages.

### 4. Configure Environment Variables (Optional)

For LLM enrichment features, you need Cloudflare Workers AI credentials:

1. Get a free account at [Cloudflare Workers AI](https://dash.cloudflare.com)
2. Create API token with appropriate permissions
3. Set environment variables:

```powershell
# PowerShell
$env:CF_ACCOUNT_ID = "your_account_id"
$env:CF_API_TOKEN = "your_api_token"

# Or for Command Prompt
set CF_ACCOUNT_ID=your_account_id
set CF_API_TOKEN=your_api_token
```

To make these permanent, add them to System Properties → Advanced → Environment Variables.

### 5. Run Dagster

```bash
uv run dagster dev
```

### 6. Access the UI

Open your web browser and navigate to:
http://localhost:3000

You can now materialize assets and explore the demo.

## Troubleshooting

### Common Issues

**"uv" command not found**
- Ensure uv was installed correctly and your terminal was restarted
- Check that the installation directory is in your PATH

**Python version errors**
- Verify you have Python 3.11+ by running `python --version`
- Install the correct version from python.org if needed

**Dependency installation failures**
- Try running the command prompt as Administrator
- Ensure you have internet connectivity for package downloads

**Lag when materializing assets**
- First runs may be slower as dependencies are cached
- Network requests to external APIs (RSS feeds, legislative data) may take time

## Project Structure

After setup, you'll see:

```
dagdemo/
├── src/dagdemo/           # Source code
│   ├── __init__.py        # Dagster Definitions
│   ├── sltrib.py          # RSS feed pipeline
│   ├── legislation.py     # Utah GLEN API ingestion
│   └── llm_enrichment.py  # Cloudflare AI enrichment
├── README.md              # This file
├── WINDOWS.md             # Windows-specific instructions (you're here)
├── pyproject.toml         # Project configuration
└── uv.lock                # Locked dependencies
```

## Next Steps

Try materializing these assets in order:
1. `sltrib_feed_xml` - Fetches SLTrib RSS feed
2. `sltrib_articles` - Parses RSS into structured articles
3. `ut_legislators` - Fetches Utah legislators data
4. `ut_bills` - Fetches Utah bills data
5. `ai_enriched_articles` - LLM summarization (requires API keys)

For workshop use, refer to [WORKSHOP.md](./WORKSHOP.md) for guided exercises.