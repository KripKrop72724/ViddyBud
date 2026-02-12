class Viddybud < Formula
  desc "Byte-perfect folder<->video codec (bytes->RGB frames->lossless MKV)"
  homepage "https://github.com/KripKrop72724/ViddyBud"
  url "https://github.com/KripKrop72724/ViddyBud/archive/refs/tags/v{{VERSION}}.tar.gz"
  sha256 "{{TARBALL_SHA256}}"
  license "MIT"

  depends_on "rust" => :build
  depends_on "ffmpeg"

  def install
    system "cargo", "install", *std_cargo_args(path: ".")
  end

  test do
    system "#{bin}/viddybud", "--version"
  end
end

