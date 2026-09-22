"""
Servidor local para el Dashboard ADECOM.
Ejecutar: python serve.py
Luego abrir: http://localhost:8765
"""
import http.server, socketserver, os, subprocess, sys

PORT = 8765
os.chdir(os.path.dirname(os.path.abspath(__file__)))

class Handler(http.server.SimpleHTTPRequestHandler):
    def log_message(self, format, *args):
        pass  # silenciar logs de cada request

print(f"Dashboard ADECOM corriendo en http://localhost:{PORT}")
print("Abre esa URL en tu navegador.")
print("Ctrl+C para detener.\n")

try:
    subprocess.Popen(["start", f"http://localhost:{PORT}"], shell=True)
except: pass

with socketserver.TCPServer(("", PORT), Handler) as httpd:
    httpd.serve_forever()
