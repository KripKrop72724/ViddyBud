{
  "version": "{{VERSION}}",
  "description": "Byte-perfect folder<->video codec (bytes->RGB frames->lossless MKV)",
  "homepage": "https://github.com/KripKrop72724/ViddyBud",
  "license": "MIT",
  "architecture": {
    "64bit": {
      "url": "https://github.com/KripKrop72724/ViddyBud/releases/download/v{{VERSION}}/viddybud-v{{VERSION}}-windows-x64.zip",
      "hash": "{{ZIP_SHA256}}"
    }
  },
  "bin": "viddybud.exe",
  "depends": [
    "ffmpeg"
  ],
  "checkver": {
    "github": "https://github.com/KripKrop72724/ViddyBud"
  },
  "autoupdate": {
    "architecture": {
      "64bit": {
        "url": "https://github.com/KripKrop72724/ViddyBud/releases/download/v$version/viddybud-v$version-windows-x64.zip"
      }
    }
  }
}

