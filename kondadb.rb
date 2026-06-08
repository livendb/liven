class Kondadb < Formula
  desc "KondaDB High-Performance Storage Engine"
  homepage "https://github.com/conduit/conduit"
  url "https://github.com/conduit/conduit/releases/download/v0.0.1/kondadb-macos-latest.tar.gz"
  sha256 "0000000000000000000000000000000000000000000000000000000000000000"
  version "0.0.1"

  def install
    bin.install "kondadb"
    etc.install "kondadb.toml" => "kondadb.toml"
  end

  def post_install
    (var/"kondadb").mkpath
  end

  service do
    run [opt_bin/"kondadb", "start", "--config", etc/"kondadb.toml"]
    keep_alive true
    run_type :immediate
    working_dir var/"kondadb"
  end

  test do
    system "#{bin}/kondadb", "--version"
  end
end
