#!/usr/bin/env python3
"""Generate self-signed TLS certificates for A2A Mesh nodes.

Creates:
- CA key + certificate (a2a-mesh-ca.key/crt)
- Nova node key + certificate signed by CA (nova.key/crt)
- Morzsa node key + certificate signed by CA (morzsa.key/crt)

Usage:
    python3 generate_certs.py [--output-dir ~/.hermes/scripts/a2a_mesh/certs]
"""
import argparse
import os
import subprocess
import sys


def run(cmd, check=True):
    """Run a shell command."""
    print(f"  $ {cmd}")
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if check and result.returncode != 0:
        print(f"  ERROR: {result.stderr}")
        sys.exit(1)
    return result


def generate_certs(output_dir):
    """Generate CA and node certificates."""
    os.makedirs(output_dir, exist_ok=True)
    
    ca_key = os.path.join(output_dir, "a2a-mesh-ca.key")
    ca_crt = os.path.join(output_dir, "a2a-mesh-ca.crt")
    
    # ── CA ──────────────────────────────────────────────────────────────────
    print("\n📜 Generating CA key and certificate...")
    run(f"openssl genrsa -out {ca_key} 4096")
    run(f"openssl req -new -x509 -key {ca_key} -out {ca_crt} "
        f"-days 3650 -subj '/CN=A2A Mesh CA/O=A2A Mesh/C=RO'")
    
    # ── Nova node ──────────────────────────────────────────────────────────
    print("\n🔑 Generating Nova node certificate...")
    nova_key = os.path.join(output_dir, "nova.key")
    nova_csr = os.path.join(output_dir, "nova.csr")
    nova_crt = os.path.join(output_dir, "nova.crt")
    
    run(f"openssl genrsa -out {nova_key} 2048")
    run(f"openssl req -new -key {nova_key} -out {nova_csr} "
        f"-subj '/CN=nova.a2a.mesh/O=A2A Mesh/C=RO'")
    
    # Add SAN extensions
    ext_file = os.path.join(output_dir, "nova.ext")
    with open(ext_file, "w") as f:
        f.write("authorityKeyIdentifier=keyid,issuer\n")
        f.write("basicConstraints=CA:FALSE\n")
        f.write("keyUsage=digitalSignature,keyEncipherment\n")
        f.write("extendedKeyUsage=serverAuth,clientAuth\n")
        f.write("subjectAltName=@alt_names\n\n")
        f.write("[alt_names]\n")
        f.write("DNS.1=nova\n")
        f.write("DNS.2=nova.a2a.mesh\n")
        f.write("DNS.3=localhost\n")
        f.write("IP.1=127.0.0.1\n")
        f.write("IP.2=192.168.1.10\n")  # Nova's IP
    
    run(f"openssl x509 -req -in {nova_csr} -CA {ca_crt} -CAkey {ca_key} "
        f"-CAcreateserial -out {nova_crt} -days 365 -extfile {ext_file}")
    
    # ── Morzsa node ────────────────────────────────────────────────────────
    print("\n🔑 Generating Morzsa node certificate...")
    morzsa_key = os.path.join(output_dir, "morzsa.key")
    morzsa_csr = os.path.join(output_dir, "morzsa.csr")
    morzsa_crt = os.path.join(output_dir, "morzsa.crt")
    
    run(f"openssl genrsa -out {morzsa_key} 2048")
    run(f"openssl req -new -key {morzsa_key} -out {morzsa_csr} "
        f"-subj '/CN=morzsa.a2a.mesh/O=A2A Mesh/C=RO'")
    
    ext_file_m = os.path.join(output_dir, "morzsa.ext")
    with open(ext_file_m, "w") as f:
        f.write("authorityKeyIdentifier=keyid,issuer\n")
        f.write("basicConstraints=CA:FALSE\n")
        f.write("keyUsage=digitalSignature,keyEncipherment\n")
        f.write("extendedKeyUsage=serverAuth,clientAuth\n")
        f.write("subjectAltName=@alt_names\n\n")
        f.write("[alt_names]\n")
        f.write("DNS.1=morzsa\n")
        f.write("DNS.2=morzsa.a2a.mesh\n")
        f.write("DNS.3=localhost\n")
        f.write("IP.1=127.0.0.1\n")
        f.write("IP.2=192.168.1.30\n")  # Morzsa's IP
    
    run(f"openssl x509 -req -in {morzsa_csr} -CA {ca_crt} -CAkey {ca_key} "
        f"-CAcreateserial -out {morzsa_crt} -days 365 -extfile {ext_file_m}")
    
    # ── Cleanup CSR and ext files ──────────────────────────────────────────
    for f in [nova_csr, morzsa_csr, ext_file, ext_file_m]:
        if os.path.exists(f):
            os.remove(f)
    
    # ── Verify ─────────────────────────────────────────────────────────────
    print("\n✅ Verifying certificates...")
    run(f"openssl verify -CAfile {ca_crt} {nova_crt}")
    run(f"openssl verify -CAfile {ca_crt} {morzsa_crt}")
    
    print(f"\n✅ Certificates generated in {output_dir}/")
    print(f"   CA:      {ca_crt}")
    print(f"   Nova:    {nova_crt} + {nova_key}")
    print(f"   Morzsa:  {morzsa_crt} + {morzsa_key}")
    print(f"\n   To enable TLS, add to mesh_config.yaml:")
    print(f"""
mesh:
  p2p:
    tls_enabled: true
    tls_cert: {nova_crt}
    tls_key: {nova_key}
    tls_ca: {ca_crt}
    tls_verify_peer: true
""")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate TLS certs for A2A Mesh")
    parser.add_argument("--output-dir", default=os.path.expanduser("~/.hermes/scripts/a2a_mesh/certs"),
                        help="Output directory for certificates")
    args = parser.parse_args()
    generate_certs(args.output_dir)