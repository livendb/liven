class Liven < Formula
  desc "LIVEN High-Performance Storage Engine"
  homepage "https://github.com/conduit/conduit"
  url "https://github.com/conduit/conduit/releases/download/v0.0.1/liven-macos-latest.tar.gz"
  sha256 "0000000000000000000000000000000000000000000000000000000000000000"
  version "0.0.1"

  def install
    bin.install "liven"
    etc.install "liven.toml" => "liven.toml"
  end

  def post_install
    (var/"liven").mkpath
  end

  service do
    run [opt_bin/"liven", "start", "--config", etc/"liven.toml"]
    keep_alive true
    run_type :immediate
    working_dir var/"liven"
  end

  test do
    system "#{bin}/liven", "--version"
  end
end
