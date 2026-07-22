#!/usr/bin/env python3
"""Generate self-signed TLS certificates for A2A Mesh nodes.

Creates:
- CA key + certificate (a2a-mesh-ca.key/crt)
- Nova node key + certificate signed by CA (nova.key/crt)
- Morzsa node key + certificate signed by CA (morzsa.key/crt)
- Runa node key + certificate signed by CA (runa.key/crt)

All certs include local IP, Tailscale IP, and localhost in SANs.

Usage:
    python3 generate_certs.py [--output-dir DIR] [--regenerate]
    python3 generate_certs.py --regenerate  # Force regeneration even if certs exist
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


# Node definitions: name -> (local_ip, tailscale_ip)
NODES = {
    "nova":   ("192.168.1.8",  "100.75.253.52"),
    "morzsa": ("192.168.1.30", "100.65.232.47"),
    "runa":   ("192.168.1.100","100.125.223.24"),
}

DAYS = 3650  # 10 years


def generate_node_cert(node_name, local_ip, tailscale_ip, ca_key, ca_crt, output_dir):
    """Generate a node certificate with SANs including all IPs."""
    key_path = os.path.join(output_dir, f"{node_name}.key")
    csr_path = os.path.join(output_dir, f"{node_name}.csr")
    crt_path = os.path.join(output_dir, f"{node_name}.crt")
    ext_path = os.path.join(output_dir, f"{node_name}_ext.cnf")
    
    print(f"\n[KEY] Generating {node_name} node certificate...")
    run(f"openssl genrsa -out {key_path} 2048")
    run(f"openssl req -new -key {key_path} -out {csr_path} "
        f"-subj '/CN={node_name}.a2a.mesh/O=A2A Mesh/C=RO'")
    
    # Build SANs with DNS names and all IPs
    san_lines = [
        "authorityKeyIdentifier=keyid,issuer",
        "basicConstraints=CA:FALSE",
        "keyUsage=digitalSignature,keyEncipherment",
        "extendedKeyUsage=serverAuth,clientAuth",
        "subjectAltName=@alt_names",
        "",
        "[alt_names]",
        f"DNS.1={node_name}",
        f"DNS.2={node_name}.a2a.mesh",
        "DNS.3=localhost",
        "IP.1=127.0.0.1",
        f"IP.2={local_ip}",
        f"IP.3={tailscale_ip}",
    ]
    
    with open(ext_path, "w") as f:
        f.write("\n".join(san_lines))
    
    run(f"openssl x509 -req -in {csr_path} -CA {ca_crt} -CAkey {ca_key} "
        f"-CAcreateserial -out {crt_path} -days {DAYS} -extfile {ext_path}")
    
    return key_path, crt_path


def generate_certs(output_dir, regenerate=False):
    """Generate CA and all node certificates."""
    os.makedirs(output_dir, exist_ok=True)
    
    ca_key = os.path.join(output_dir, "a2a-mesh-ca.key")
    ca_crt = os.path.join(output_dir, "a2a-mesh-ca.crt")
    
    # -- CA --
    if not os.path.exists(ca_key) or regenerate:
        print("\n[CA] Generating CA key and certificate...")
        run(f"openssl genrsa -out {ca_key} 4096")
        run(f"openssl req -new -x509 -key {ca_key} -out {ca_crt} "
            f"-days {DAYS} -subj '/CN=A2A Mesh CA/O=A2A Mesh/C=RO'")
    else:
        print("\n[CA] CA certificate already exists, skipping...")
    
    # -- Generate certs for all nodes --
    certs_info = {}
    for node_name, (local_ip, tailscale_ip) in NODES.items():
        key_path = os.path.join(output_dir, f"{node_name}.key")
        crt_path = os.path.join(output_dir, f"{node_name}.crt")
        
        if os.path.exists(key_path) and os.path.exists(crt_path) and not regenerate:
            print(f"\n[OK] {node_name} certificate already exists, skipping...")
            result = run(f"openssl verify -CAfile {ca_crt} {crt_path}", check=False)
            if result.returncode != 0:
                print(f"  [!] {node_name} cert verification failed, regenerating...")
                key_path, crt_path = generate_node_cert(
                    node_name, local_ip, tailscale_ip, ca_key, ca_crt, output_dir
                )
        else:
            key_path, crt_path = generate_node_cert(
                node_name, local_ip, tailscale_ip, ca_key, ca_crt, output_dir
            )
        
        certs_info[node_name] = {"key": key_path, "crt": crt_path, "local_ip": local_ip, "tailscale_ip": tailscale_ip}
    
    # -- Cleanup CSRs and ext files --
    for node_name in NODES:
        for ext in ["csr", "ext.cnf"]:
            f = os.path.join(output_dir, f"{node_name}.{ext}")
            if os.path.exists(f):
                os.remove(f)
    
    # -- Verify --
    print("\n[VERIFY] Verifying certificates...")
    for node_name in NODES:
        crt = certs_info[node_name]["crt"]
        run(f"openssl verify -CAfile {ca_crt} {crt}")
    
    # -- Summary --
    print(f"\n[DONE] Certificates generated in {output_dir}/")
    print(f"   CA: {ca_crt}")
    for node_name, info in certs_info.items():
        print(f"   {node_name}: {info['crt']} + {info['key']}")
    
    print(f"\n   To enable TLS on each node, add to mesh_config.yaml:")
    print(f"""
mesh:
  transports:
    p2p:
      tls_enabled: true
      tls_cert: {os.path.join(output_dir, 'NODE_NAME.crt')}
      tls_key: {os.path.join(output_dir, 'NODE_NAME.key')}
      tls_ca: {ca_crt}
      tls_verify_peer: true
""")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate TLS certs for A2A Mesh")
    parser.add_argument("--output-dir", default=os.path.expanduser("~/.hermes/scripts/a2a_mesh/certs"),
                        help="Output directory for certificates")
    parser.add_argument("--regenerate", action="store_true",
                        help="Force regeneration even if certs exist")
    args = parser.parse_args()
    generate_certs(args.output_dir, regenerate=args.regenerate)
