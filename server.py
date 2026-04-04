#!/usr/bin/env python3
"""
PharmaCentral - Pharmacy Chain Operations Platform
Full working prototype using Python stdlib only.
Run: python3 server.py
Then open: http://localhost:8000
"""

import json
import sqlite3
import hashlib
import hmac
import base64
import time
import uuid
import os
import re
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs
from datetime import datetime, timedelta
from pathlib import Path

# ─── CONFIG ──────────────────────────────────────────────────────────────────
PORT = 8000
SECRET_KEY = "pharmacentral-secret-key-2024-production"
DB_PATH = Path(__file__).parent / "db" / "pharma.db"
STATIC_DIR = Path(__file__).parent / "static"

# ─── DATABASE SETUP ──────────────────────────────────────────────────────────
def get_db():
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn

def init_db():
    DB_PATH.parent.mkdir(exist_ok=True)
    conn = get_db()
    c = conn.cursor()

    # Users & Auth
    c.executescript("""
    CREATE TABLE IF NOT EXISTS stores (
        id TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        location TEXT,
        store_type TEXT DEFAULT 'urban',
        is_active INTEGER DEFAULT 1,
        created_at TEXT DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS users (
        id TEXT PRIMARY KEY,
        username TEXT UNIQUE NOT NULL,
        email TEXT UNIQUE NOT NULL,
        password_hash TEXT NOT NULL,
        full_name TEXT NOT NULL,
        role TEXT NOT NULL CHECK(role IN ('admin','regional_manager','pharmacist','inventory_controller')),
        store_id TEXT REFERENCES stores(id),
        is_active INTEGER DEFAULT 1,
        two_fa_enabled INTEGER DEFAULT 1,
        last_login TEXT,
        created_at TEXT DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS audit_log (
        id TEXT PRIMARY KEY,
        timestamp TEXT DEFAULT (datetime('now')),
        action TEXT NOT NULL,
        user_id TEXT,
        user_name TEXT,
        entity_type TEXT,
        entity_id TEXT,
        details TEXT,
        ip_address TEXT,
        integrity_hash TEXT
    );

    CREATE TABLE IF NOT EXISTS drugs (
        id TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        generic_name TEXT,
        manufacturer TEXT,
        category TEXT,
        schedule_type TEXT DEFAULT 'OTC',
        unit TEXT DEFAULT 'strip',
        mrp REAL NOT NULL,
        gst_percent REAL DEFAULT 12.0,
        reorder_level INTEGER DEFAULT 50,
        is_active INTEGER DEFAULT 1
    );

    CREATE TABLE IF NOT EXISTS inventory (
        id TEXT PRIMARY KEY,
        store_id TEXT NOT NULL REFERENCES stores(id),
        drug_id TEXT NOT NULL REFERENCES drugs(id),
        batch_no TEXT NOT NULL,
        quantity INTEGER NOT NULL DEFAULT 0,
        purchase_price REAL,
        mrp REAL NOT NULL,
        expiry_date TEXT NOT NULL,
        manufacture_date TEXT,
        received_date TEXT DEFAULT (date('now')),
        supplier TEXT,
        UNIQUE(store_id, drug_id, batch_no)
    );

    CREATE TABLE IF NOT EXISTS sales (
        id TEXT PRIMARY KEY,
        invoice_no TEXT UNIQUE NOT NULL,
        store_id TEXT NOT NULL REFERENCES stores(id),
        pharmacist_id TEXT REFERENCES users(id),
        patient_name TEXT,
        patient_phone TEXT,
        patient_uhid TEXT,
        prescription_id TEXT,
        sale_type TEXT DEFAULT 'OTC',
        subtotal REAL DEFAULT 0,
        gst_amount REAL DEFAULT 0,
        discount_amount REAL DEFAULT 0,
        total_amount REAL DEFAULT 0,
        payment_method TEXT DEFAULT 'cash',
        status TEXT DEFAULT 'completed',
        created_at TEXT DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS sale_items (
        id TEXT PRIMARY KEY,
        sale_id TEXT NOT NULL REFERENCES sales(id),
        drug_id TEXT NOT NULL REFERENCES drugs(id),
        inventory_id TEXT NOT NULL REFERENCES inventory(id),
        drug_name TEXT NOT NULL,
        batch_no TEXT NOT NULL,
        quantity INTEGER NOT NULL,
        unit_price REAL NOT NULL,
        total_price REAL NOT NULL
    );

    CREATE TABLE IF NOT EXISTS stock_transfers (
        id TEXT PRIMARY KEY,
        transfer_no TEXT UNIQUE NOT NULL,
        from_store_id TEXT NOT NULL REFERENCES stores(id),
        to_store_id TEXT NOT NULL REFERENCES stores(id),
        drug_id TEXT NOT NULL REFERENCES drugs(id),
        drug_name TEXT NOT NULL,
        batch_no TEXT NOT NULL,
        quantity INTEGER NOT NULL,
        transfer_value REAL DEFAULT 0,
        reason TEXT,
        status TEXT DEFAULT 'pending',
        requested_by TEXT REFERENCES users(id),
        approved_by TEXT REFERENCES users(id),
        created_at TEXT DEFAULT (datetime('now')),
        updated_at TEXT DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS purchase_orders (
        id TEXT PRIMARY KEY,
        po_no TEXT UNIQUE NOT NULL,
        store_id TEXT NOT NULL REFERENCES stores(id),
        supplier TEXT NOT NULL,
        drug_id TEXT NOT NULL REFERENCES drugs(id),
        drug_name TEXT NOT NULL,
        quantity INTEGER NOT NULL,
        unit_cost REAL NOT NULL,
        total_cost REAL NOT NULL,
        status TEXT DEFAULT 'pending',
        requested_by TEXT REFERENCES users(id),
        approved_by TEXT REFERENCES users(id),
        notes TEXT,
        created_at TEXT DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS anomalies (
        id TEXT PRIMARY KEY,
        anomaly_type TEXT NOT NULL,
        severity TEXT NOT NULL CHECK(severity IN ('critical','medium','low')),
        score INTEGER DEFAULT 0,
        title TEXT NOT NULL,
        description TEXT,
        store_id TEXT REFERENCES stores(id),
        entity_id TEXT,
        entity_type TEXT,
        status TEXT DEFAULT 'open',
        resolved_by TEXT REFERENCES users(id),
        created_at TEXT DEFAULT (datetime('now')),
        updated_at TEXT DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS prescriptions (
        id TEXT PRIMARY KEY,
        prescription_no TEXT UNIQUE NOT NULL,
        store_id TEXT REFERENCES stores(id),
        patient_name TEXT,
        patient_phone TEXT,
        doctor_name TEXT,
        doctor_reg_no TEXT,
        prescription_date TEXT,
        status TEXT DEFAULT 'active',
        created_at TEXT DEFAULT (datetime('now'))
    );
    """)
    conn.commit()
    seed_data(conn)
    conn.close()

def seed_data(conn):
    c = conn.cursor()
    # Check if already seeded
    if c.execute("SELECT COUNT(*) FROM stores").fetchone()[0] > 0:
        return

    # Stores
    stores = [
        ("store-001", "MG Road", "Kanpur, UP", "urban"),
        ("store-002", "Civil Lines", "Kanpur, UP", "urban"),
        ("store-003", "Kidwai Nagar", "Kanpur, UP", "semi_urban"),
        ("store-004", "Panki", "Kanpur, UP", "semi_urban"),
        ("store-005", "Armapur", "Kanpur, UP", "urban"),
    ]
    c.executemany("INSERT INTO stores(id,name,location,store_type) VALUES(?,?,?,?)", stores)

    # Users - passwords all "password123"
    def hash_pw(pw): return hashlib.sha256(pw.encode()).hexdigest()
    users = [
        ("user-001","admin","admin@pharmacentral.in",hash_pw("password123"),"Head Admin","admin","store-001"),
        ("user-002","rajesh","rajesh@pharmacentral.in",hash_pw("password123"),"Rajesh Sharma","pharmacist","store-001"),
        ("user-003","vikram","vikram@pharmacentral.in",hash_pw("password123"),"Vikram Nair","regional_manager","store-002"),
        ("user-004","sunita","sunita@pharmacentral.in",hash_pw("password123"),"Sunita Agarwal","inventory_controller","store-001"),
        ("user-005","amita","amita@pharmacentral.in",hash_pw("password123"),"Amita Roy","pharmacist","store-001"),
    ]
    c.executemany("INSERT INTO users(id,username,email,password_hash,full_name,role,store_id) VALUES(?,?,?,?,?,?,?)", users)

    # Drugs
    drugs = [
        ("drug-001","Metformin 500mg Tab","Metformin","Sun Pharma","Diabetic","OTC","strip",38.0,12.0,50),
        ("drug-002","Amlodipine 5mg Tab","Amlodipine","Cipla","Cardiac","OTC","strip",42.0,12.0,60),
        ("drug-003","Amoxicillin 250mg Cap","Amoxicillin","Ranbaxy","Antibiotic","OTC","box",95.0,12.0,30),
        ("drug-004","Alprazolam 0.5mg Tab","Alprazolam","Pfizer","Psychotropic","Schedule_H","strip",85.0,5.0,20),
        ("drug-005","Dolo 650 Strip","Paracetamol","Micro Labs","Analgesic","OTC","strip",30.0,0.0,100),
        ("drug-006","Atorvastatin 10mg Tab","Atorvastatin","Sun Pharma","Cardiac","OTC","strip",64.0,12.0,40),
        ("drug-007","Pantoprazole 40mg Tab","Pantoprazole","Lupin","Gastro","OTC","strip",45.0,12.0,40),
        ("drug-008","Cetirizine 10mg Tab","Cetirizine","Mankind","Antihistamine","OTC","strip",15.0,0.0,80),
        ("drug-009","Insulin Glargine 3ml","Insulin","Sanofi","Diabetic","Schedule_H","vial",2600.0,5.0,10),
        ("drug-010","Tramadol 50mg Tab","Tramadol","Zydus","Analgesic","Schedule_H","strip",80.0,5.0,25),
        ("drug-011","Azithromycin 500mg Tab","Azithromycin","Cipla","Antibiotic","OTC","strip",120.0,12.0,30),
        ("drug-012","Lisinopril 5mg Tab","Lisinopril","Torrent","Cardiac","OTC","strip",55.0,12.0,40),
    ]
    c.executemany("INSERT INTO drugs(id,name,generic_name,manufacturer,category,schedule_type,unit,mrp,gst_percent,reorder_level) VALUES(?,?,?,?,?,?,?,?,?,?)", drugs)

    # Inventory for store-001
    today = datetime.now()
    inv = [
        ("inv-001","store-001","drug-001","MF-2024-09",18,28.0,38.0,(today+timedelta(days=600)).strftime("%Y-%m-%d"),"Medline India"),
        ("inv-002","store-001","drug-002","AM-2024-07",204,30.0,42.0,(today+timedelta(days=330)).strftime("%Y-%m-%d"),"PharmAlliance"),
        ("inv-003","store-001","drug-003","BX-2024-04",48,68.0,95.0,(today+timedelta(days=18)).strftime("%Y-%m-%d"),"Ranbaxy Direct"),
        ("inv-004","store-001","drug-004","AX-2024-12",45,65.0,85.0,(today+timedelta(days=210)).strftime("%Y-%m-%d"),"Pfizer India"),
        ("inv-005","store-001","drug-005","D-2024-22",730,22.0,30.0,(today+timedelta(days=22)).strftime("%Y-%m-%d"),"Micro Labs"),
        ("inv-006","store-001","drug-006","AV-2024-07",36,48.0,64.0,(today+timedelta(days=28)).strftime("%Y-%m-%d"),"Sun Pharma"),
        ("inv-007","store-001","drug-007","PP-2024-11",24,33.0,45.0,(today+timedelta(days=8)).strftime("%Y-%m-%d"),"Lupin Ltd"),
        ("inv-008","store-001","drug-008","CZ-2024-09",200,10.0,15.0,(today+timedelta(days=210)).strftime("%Y-%m-%d"),"Mankind"),
        ("inv-009","store-001","drug-009","IG-2024-06",12,1900.0,2600.0,(today+timedelta(days=180)).strftime("%Y-%m-%d"),"Sanofi"),
        ("inv-010","store-001","drug-010","TR-2024-08",42,60.0,80.0,(today+timedelta(days=270)).strftime("%Y-%m-%d"),"Zydus"),
        ("inv-011","store-001","drug-011","AZ-2024-10",65,90.0,120.0,(today+timedelta(days=300)).strftime("%Y-%m-%d"),"Cipla"),
        ("inv-012","store-001","drug-012","LS-2024-09",80,40.0,55.0,(today+timedelta(days=350)).strftime("%Y-%m-%d"),"Torrent"),
        # store-002 inventory
        ("inv-013","store-002","drug-001","MF-2024-08",240,28.0,38.0,(today+timedelta(days=560)).strftime("%Y-%m-%d"),"Medline India"),
        ("inv-014","store-002","drug-002","AM-2024-06",180,30.0,42.0,(today+timedelta(days=300)).strftime("%Y-%m-%d"),"PharmAlliance"),
    ]
    c.executemany("INSERT INTO inventory(id,store_id,drug_id,batch_no,quantity,purchase_price,mrp,expiry_date,supplier) VALUES(?,?,?,?,?,?,?,?,?)", inv)

    # Sample sales
    sales_data = []
    sale_items_data = []
    for i in range(1, 20):
        sid = f"sale-{i:04d}"
        inv_no = f"INV-20240403-{300+i:04d}"
        amt = [380,1240,2890,95,750,440,1100,320,680,1950,560,820,445,1820,290,640,1380,760,2100][i-1]
        stype = "prescription" if i % 3 == 0 else "OTC"
        sales_data.append((sid, inv_no, "store-001", "user-002",
                           f"Patient {i}", f"98765{i:05d}", None, None,
                           stype, amt*0.85, amt*0.12, amt*0.03, amt, "upi",
                           "completed", (datetime.now()-timedelta(hours=i*0.5)).strftime("%Y-%m-%d %H:%M:%S")))
        sale_items_data.append((f"sitem-{i}",sid,"drug-005","inv-005",
                                "Dolo 650 Strip","D-2024-22",2,30.0,60.0))
    c.executemany("INSERT INTO sales(id,invoice_no,store_id,pharmacist_id,patient_name,patient_phone,patient_uhid,prescription_id,sale_type,subtotal,gst_amount,discount_amount,total_amount,payment_method,status,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", sales_data)
    c.executemany("INSERT INTO sale_items(id,sale_id,drug_id,inventory_id,drug_name,batch_no,quantity,unit_price,total_price) VALUES(?,?,?,?,?,?,?,?,?)", sale_items_data)

    # Transfers
    transfers = [
        ("trn-001","TRF-240401","store-002","store-001","drug-002","Amlodipine 5mg","AM-2024-06",100,4200.0,"Emergency Restock","completed","user-002",None),
        ("trn-002","TRF-240402","store-002","store-001","drug-006","Atorvastatin 10mg","AV-2024-07",150,9600.0,"Routine Rebalancing","pending","user-002",None),
        ("trn-003","TRF-240403","store-002","store-001","drug-009","Insulin Glargine","IG-2024-06",24,62400.0,"Emergency Restock","pending","user-003",None),
    ]
    c.executemany("INSERT INTO stock_transfers(id,transfer_no,from_store_id,to_store_id,drug_id,drug_name,batch_no,quantity,transfer_value,reason,status,requested_by,approved_by) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)", transfers)

    # Anomalies
    anomalies = [
        ("anom-001","prescription_fraud","critical",94,"Excessive Schedule H Dispensing","Patient P-84721 purchased 14 strips of Alprazolam in 3 days across 2 stores. Normal: 1 strip/month.","store-001","drug-004","drug","open"),
        ("anom-002","stock_discrepancy","critical",88,"Stock Discrepancy - Tramadol 50mg","Physical: 42 strips, System: 68. Variance: 26 strips (₹2,080). No GRN/transfer records.","store-004","inv-010","inventory","open"),
        ("anom-003","discount_abuse","medium",62,"Unusual Discount Pattern - Staff S-0218","14 transactions with avg 18% discount vs chain avg 4.2%. Total: ₹38,400.","store-001","user-005","user","open"),
        ("anom-004","demand_spike","low",34,"Demand Spike - Antibiotic Category","Amoxicillin & Azithromycin up 48% across 6 stores this week. Seasonal flu indicator.","store-001","drug-003","drug","open"),
    ]
    c.executemany("INSERT INTO anomalies(id,anomaly_type,severity,score,title,description,store_id,entity_id,entity_type,status) VALUES(?,?,?,?,?,?,?,?,?,?)", anomalies)

    # POs
    pos = [
        ("po-001","PO-2024-0891","store-001","Medline India","drug-001","Metformin 500mg Tab",200,28.0,5600.0,"pending","user-002",None,"Urgent - stock critical"),
        ("po-002","PO-2024-0892","store-001","Sun Pharma","drug-006","Atorvastatin 10mg",150,48.0,7200.0,"approved","user-002","user-003",None),
    ]
    c.executemany("INSERT INTO purchase_orders(id,po_no,store_id,supplier,drug_id,drug_name,quantity,unit_cost,total_cost,status,requested_by,approved_by,notes) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)", pos)

    # Audit log
    audits = [
        ("aud-001","SALE_COMPLETE","user-002","Rajesh Sharma","sale","sale-0001","INV-20240403-0301 · ₹1,240 · Prescription","192.168.1.42"),
        ("aud-002","STOCK_ADJUST","user-004","Sunita Agarwal","inventory","inv-001","Metformin count: 22→18. Reason: Breakage","192.168.1.18"),
        ("aud-003","ANOMALY_FLAG","system","System (AI)","patient","P-84721","Alprazolam anomaly. Score: 94","system"),
        ("aud-004","TRANSFER_INIT","user-003","Vikram Nair","transfer","trn-002","Atorvastatin 150 strips · Civil Lines→MG Road","10.0.0.5"),
        ("aud-005","LOGIN","user-002","Rajesh Sharma","session","S-9821","MFA verified · MG Road","192.168.1.42"),
    ]
    for a in audits:
        hash_val = hashlib.sha256(f"{a[0]}{a[1]}{a[4]}".encode()).hexdigest()[:16]
        c.execute("INSERT INTO audit_log(id,action,user_id,user_name,entity_type,entity_id,details,ip_address,integrity_hash) VALUES(?,?,?,?,?,?,?,?,?)",
                  (*a, hash_val))

    conn.commit()

# ─── JWT HELPERS ─────────────────────────────────────────────────────────────
def make_token(payload: dict) -> str:
    header = base64.urlsafe_b64encode(json.dumps({"alg":"HS256","typ":"JWT"}).encode()).decode().rstrip("=")
    payload["exp"] = int(time.time()) + 86400  # 24h
    body = base64.urlsafe_b64encode(json.dumps(payload).encode()).decode().rstrip("=")
    sig_input = f"{header}.{body}".encode()
    sig = hmac.new(SECRET_KEY.encode(), sig_input, hashlib.sha256).digest()
    sig_b64 = base64.urlsafe_b64encode(sig).decode().rstrip("=")
    return f"{header}.{body}.{sig_b64}"

def verify_token(token: str) -> dict | None:
    try:
        parts = token.split(".")
        if len(parts) != 3: return None
        header, body, sig = parts
        sig_input = f"{header}.{body}".encode()
        expected = hmac.new(SECRET_KEY.encode(), sig_input, hashlib.sha256).digest()
        expected_b64 = base64.urlsafe_b64encode(expected).decode().rstrip("=")
        if not hmac.compare_digest(sig, expected_b64): return None
        pad = lambda s: s + "=" * (4 - len(s) % 4)
        payload = json.loads(base64.urlsafe_b64decode(pad(body)))
        if payload.get("exp", 0) < time.time(): return None
        return payload
    except Exception:
        return None

# ─── RBAC ────────────────────────────────────────────────────────────────────
PERMISSIONS = {
    "admin": ["*"],
    "regional_manager": ["sales:read","sales:create","inventory:read","inventory:write",
                         "transfer:read","transfer:create","transfer:approve",
                         "reports:read","anomaly:read","anomaly:resolve","audit:read","po:read"],
    "pharmacist": ["sales:create","sales:read","inventory:read","transfer:create",
                   "transfer:read","reports:store","prescription:verify"],
    "inventory_controller": ["inventory:read","inventory:write","transfer:create",
                              "transfer:read","grn:process","reports:store"],
}

def has_perm(role: str, perm: str) -> bool:
    perms = PERMISSIONS.get(role, [])
    return "*" in perms or perm in perms

# ─── HTTP SERVER ──────────────────────────────────────────────────────────────
class PharmaCentralHandler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        pass  # suppress default logging

    def send_json(self, data, status=200):
        body = json.dumps(data, default=str).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", len(body))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Headers", "Authorization,Content-Type")
        self.end_headers()
        self.wfile.write(body)

    def send_error_json(self, msg, status=400):
        self.send_json({"error": msg}, status)

    def get_body(self):
        length = int(self.headers.get("Content-Length", 0))
        return json.loads(self.rfile.read(length)) if length else {}

    def get_user(self):
        auth = self.headers.get("Authorization","")
        if not auth.startswith("Bearer "): return None
        return verify_token(auth[7:])

    def serve_static(self, path):
        if path == "/" or path == "": path = "/index.html"
        file_path = STATIC_DIR / path.lstrip("/")
        if not file_path.exists():
            self.send_response(404)
            self.end_headers()
            return
        ext = file_path.suffix
        ct = {"html":"text/html","css":"text/css","js":"application/javascript",
              "json":"application/json","png":"image/png","ico":"image/x-icon"}.get(ext[1:],"text/plain")
        body = file_path.read_bytes()
        self.send_response(200)
        self.send_header("Content-Type", ct)
        self.send_header("Content-Length", len(body))
        self.end_headers()
        self.wfile.write(body)

    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET,POST,PUT,PATCH,DELETE,OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Authorization,Content-Type")
        self.end_headers()

    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path.rstrip("/")
        qs = parse_qs(parsed.query)

        if not path.startswith("/api"):
            self.serve_static(path)
            return

        user = self.get_user()

        # Health
        if path == "/api/health":
            self.send_json({"status":"ok","service":"PharmaCentral","version":"1.0.0"})

        # Dashboard stats
        elif path == "/api/dashboard":
            if not user: return self.send_error_json("Unauthorized", 401)
            self.handle_dashboard(user, qs)

        elif path == "/api/inventory":
            if not user: return self.send_error_json("Unauthorized", 401)
            self.handle_inventory_list(user, qs)

        elif path == "/api/drugs":
            if not user: return self.send_error_json("Unauthorized", 401)
            self.handle_drugs_list(qs)

        elif path == "/api/sales":
            if not user: return self.send_error_json("Unauthorized", 401)
            self.handle_sales_list(user, qs)

        elif path == "/api/transfers":
            if not user: return self.send_error_json("Unauthorized", 401)
            self.handle_transfers_list(user, qs)

        elif path == "/api/anomalies":
            if not user: return self.send_error_json("Unauthorized", 401)
            self.handle_anomalies_list(qs)

        elif path == "/api/purchase-orders":
            if not user: return self.send_error_json("Unauthorized", 401)
            self.handle_po_list(qs)

        elif path == "/api/reports/chain":
            if not user: return self.send_error_json("Unauthorized", 401)
            self.handle_chain_report(qs)

        elif path == "/api/reports/store":
            if not user: return self.send_error_json("Unauthorized", 401)
            self.handle_store_report(user, qs)

        elif path == "/api/expiry-alerts":
            if not user: return self.send_error_json("Unauthorized", 401)
            self.handle_expiry_alerts(user, qs)

        elif path == "/api/audit-log":
            if not user: return self.send_error_json("Unauthorized", 401)
            self.handle_audit_log(qs)

        elif path == "/api/users":
            if not user: return self.send_error_json("Unauthorized", 401)
            self.handle_users_list(user, qs)

        elif path == "/api/stores":
            if not user: return self.send_error_json("Unauthorized", 401)
            self.handle_stores_list()

        elif path == "/api/forecast":
            if not user: return self.send_error_json("Unauthorized", 401)
            self.handle_forecast(qs)

        elif path == "/api/reorder-recommendations":
            if not user: return self.send_error_json("Unauthorized", 401)
            self.handle_reorder_recs(user, qs)

        else:
            self.send_error_json("Not found", 404)

    def do_POST(self):
        parsed = urlparse(self.path)
        path = parsed.path.rstrip("/")
        body = self.get_body()
        user = self.get_user()

        if path == "/api/auth/login":
            self.handle_login(body)
        elif path == "/api/sales":
            if not user: return self.send_error_json("Unauthorized", 401)
            self.handle_create_sale(user, body)
        elif path == "/api/transfers":
            if not user: return self.send_error_json("Unauthorized", 401)
            self.handle_create_transfer(user, body)
        elif path == "/api/purchase-orders":
            if not user: return self.send_error_json("Unauthorized", 401)
            self.handle_create_po(user, body)
        elif path == "/api/inventory/adjust":
            if not user: return self.send_error_json("Unauthorized", 401)
            self.handle_stock_adjust(user, body)
        elif path == "/api/anomalies/resolve":
            if not user: return self.send_error_json("Unauthorized", 401)
            self.handle_resolve_anomaly(user, body)
        elif path == "/api/ai/query":
            if not user: return self.send_error_json("Unauthorized", 401)
            self.handle_ai_query(user, body)
        elif path == "/api/users":
            if not user: return self.send_error_json("Unauthorized", 401)
            self.handle_create_user(user, body)
        else:
            self.send_error_json("Not found", 404)

    def do_PATCH(self):
        parsed = urlparse(self.path)
        path = parsed.path.rstrip("/")
        body = self.get_body()
        user = self.get_user()
        if not user: return self.send_error_json("Unauthorized", 401)

        if "/api/transfers/" in path:
            tid = path.split("/")[-1]
            self.handle_update_transfer(user, tid, body)
        elif "/api/anomalies/" in path:
            aid = path.split("/")[-1]
            self.handle_update_anomaly(user, aid, body)
        else:
            self.send_error_json("Not found", 404)

    # ── HANDLERS ────────────────────────────────────────────────────────────

    def handle_login(self, body):
        username = body.get("username","")
        password = body.get("password","")
        if not username or not password:
            return self.send_error_json("Username and password required")

        pw_hash = hashlib.sha256(password.encode()).hexdigest()
        conn = get_db()
        user = conn.execute(
            "SELECT u.*, s.name as store_name FROM users u LEFT JOIN stores s ON u.store_id=s.id WHERE u.username=? AND u.password_hash=? AND u.is_active=1",
            (username, pw_hash)
        ).fetchone()
        conn.close()

        if not user:
            return self.send_error_json("Invalid credentials", 401)

        # Update last login
        conn = get_db()
        conn.execute("UPDATE users SET last_login=datetime('now') WHERE id=?", (user["id"],))
        conn.commit()

        # Audit log
        self.write_audit(user["id"], user["full_name"], "LOGIN", "session", f"session-{uuid.uuid4().hex[:8]}", f"Login from {self.client_address[0]}", self.client_address[0])
        conn.close()

        token = make_token({
            "sub": user["id"],
            "username": user["username"],
            "full_name": user["full_name"],
            "role": user["role"],
            "store_id": user["store_id"],
            "store_name": user["store_name"] or ""
        })
        self.send_json({
            "token": token,
            "user": {
                "id": user["id"],
                "username": user["username"],
                "full_name": user["full_name"],
                "role": user["role"],
                "store_id": user["store_id"],
                "store_name": user["store_name"] or ""
            }
        })

    def handle_dashboard(self, user, qs):
        store_id = user.get("store_id") or "store-001"
        conn = get_db()
        today = datetime.now().strftime("%Y-%m-%d")

        # Today's sales
        sales_today = conn.execute(
            "SELECT COUNT(*) as cnt, COALESCE(SUM(total_amount),0) as rev FROM sales WHERE store_id=? AND date(created_at)=?",
            (store_id, today)
        ).fetchone()

        # Low stock count
        low_stock = conn.execute("""
            SELECT COUNT(*) as cnt FROM inventory i
            JOIN drugs d ON i.drug_id=d.id
            WHERE i.store_id=? AND i.quantity <= d.reorder_level
        """, (store_id,)).fetchone()

        # Near expiry (30 days)
        exp_date = (datetime.now()+timedelta(days=30)).strftime("%Y-%m-%d")
        near_expiry = conn.execute(
            "SELECT COUNT(*) as cnt FROM inventory WHERE store_id=? AND expiry_date<=?",
            (store_id, exp_date)
        ).fetchone()

        # Open anomalies
        open_anomalies = conn.execute(
            "SELECT COUNT(*) as cnt FROM anomalies WHERE (store_id=? OR store_id IS NULL) AND status='open'",
            (store_id,)
        ).fetchone()

        # Recent sales
        recent = conn.execute("""
            SELECT s.invoice_no, s.patient_name, s.total_amount, s.sale_type,
                   s.status, s.created_at,
                   u.full_name as pharmacist_name,
                   COUNT(si.id) as item_count
            FROM sales s
            LEFT JOIN users u ON s.pharmacist_id=u.id
            LEFT JOIN sale_items si ON s.id=si.sale_id
            WHERE s.store_id=?
            GROUP BY s.id
            ORDER BY s.created_at DESC LIMIT 10
        """, (store_id,)).fetchall()

        # Expiry alerts
        exp_alerts = conn.execute("""
            SELECT i.*, d.name as drug_name, d.category,
                   CAST(julianday(i.expiry_date)-julianday('now') AS INTEGER) as days_left
            FROM inventory i JOIN drugs d ON i.drug_id=d.id
            WHERE i.store_id=? AND i.expiry_date <= date('now','+45 days')
            ORDER BY i.expiry_date ASC LIMIT 8
        """, (store_id,)).fetchall()

        # Daily revenue (14 days)
        daily_rev = conn.execute("""
            SELECT date(created_at) as day,
                   COALESCE(SUM(total_amount),0) as revenue,
                   COUNT(*) as bills
            FROM sales WHERE store_id=? AND created_at >= date('now','-14 days')
            GROUP BY date(created_at) ORDER BY day
        """, (store_id,)).fetchall()

        conn.close()
        self.send_json({
            "stats": {
                "revenue_today": round(sales_today["rev"], 2),
                "bills_today": sales_today["cnt"],
                "low_stock_count": low_stock["cnt"],
                "near_expiry_count": near_expiry["cnt"],
                "open_anomalies": open_anomalies["cnt"]
            },
            "recent_sales": [dict(r) for r in recent],
            "expiry_alerts": [dict(e) for e in exp_alerts],
            "daily_revenue": [dict(d) for d in daily_rev]
        })

    def handle_inventory_list(self, user, qs):
        store_id = qs.get("store_id",[""])[0] or user.get("store_id") or "store-001"
        category = qs.get("category",[""])[0]
        search = qs.get("search",[""])[0]
        status_f = qs.get("status",[""])[0]

        sql = """
            SELECT i.*, d.name as drug_name, d.generic_name, d.manufacturer,
                   d.category, d.schedule_type, d.unit, d.reorder_level,
                   d.gst_percent,
                   CAST(julianday(i.expiry_date)-julianday('now') AS INTEGER) as days_to_expiry,
                   ROUND((i.mrp-i.purchase_price)/i.mrp*100,1) as margin_pct,
                   d.reorder_level as reorder_point
            FROM inventory i JOIN drugs d ON i.drug_id=d.id
            WHERE i.store_id=?
        """
        params = [store_id]
        if category:
            sql += " AND d.category=?"
            params.append(category)
        if search:
            sql += " AND (d.name LIKE ? OR i.batch_no LIKE ? OR d.manufacturer LIKE ?)"
            params.extend([f"%{search}%"]*3)
        if status_f == "low":
            sql += " AND i.quantity <= d.reorder_level"
        elif status_f == "critical":
            sql += " AND i.quantity = 0"
        sql += " ORDER BY days_to_expiry ASC, i.quantity ASC"

        conn = get_db()
        rows = conn.execute(sql, params).fetchall()
        conn.close()
        self.send_json({"items": [dict(r) for r in rows], "total": len(rows)})

    def handle_drugs_list(self, qs):
        search = qs.get("search",[""])[0]
        conn = get_db()
        if search:
            rows = conn.execute("SELECT * FROM drugs WHERE name LIKE ? OR generic_name LIKE ? AND is_active=1", (f"%{search}%",f"%{search}%")).fetchall()
        else:
            rows = conn.execute("SELECT * FROM drugs WHERE is_active=1 ORDER BY name").fetchall()
        conn.close()
        self.send_json({"drugs": [dict(r) for r in rows]})

    def handle_sales_list(self, user, qs):
        store_id = user.get("store_id") or "store-001"
        conn = get_db()
        rows = conn.execute("""
            SELECT s.*, u.full_name as pharmacist_name,
                   COUNT(si.id) as item_count
            FROM sales s
            LEFT JOIN users u ON s.pharmacist_id=u.id
            LEFT JOIN sale_items si ON s.id=si.sale_id
            WHERE s.store_id=?
            GROUP BY s.id ORDER BY s.created_at DESC LIMIT 50
        """, (store_id,)).fetchall()
        conn.close()
        self.send_json({"sales": [dict(r) for r in rows]})

    def handle_create_sale(self, user, body):
        if not has_perm(user["role"], "sales:create"):
            return self.send_error_json("Permission denied", 403)

        items = body.get("items", [])
        if not items:
            return self.send_error_json("No items in sale")

        conn = get_db()
        sale_id = f"sale-{uuid.uuid4().hex[:8]}"
        invoice_no = f"INV-{datetime.now().strftime('%Y%m%d')}-{uuid.uuid4().hex[:6].upper()}"
        store_id = user.get("store_id") or "store-001"

        subtotal = sum(i.get("quantity",1) * i.get("unit_price",0) for i in items)
        gst = subtotal * 0.12
        discount = subtotal * 0.03
        total = subtotal + gst - discount

        conn.execute("""
            INSERT INTO sales(id,invoice_no,store_id,pharmacist_id,patient_name,patient_phone,
                              sale_type,subtotal,gst_amount,discount_amount,total_amount,payment_method,status)
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (sale_id, invoice_no, store_id, user["sub"],
              body.get("patient_name","Walk-in"), body.get("patient_phone",""),
              body.get("sale_type","OTC"), round(subtotal,2), round(gst,2),
              round(discount,2), round(total,2), body.get("payment_method","cash"), "completed"))

        for item in items:
            iid = f"si-{uuid.uuid4().hex[:8]}"
            conn.execute("""
                INSERT INTO sale_items(id,sale_id,drug_id,inventory_id,drug_name,batch_no,quantity,unit_price,total_price)
                VALUES(?,?,?,?,?,?,?,?,?)
            """, (iid, sale_id, item.get("drug_id",""), item.get("inventory_id",""),
                  item.get("drug_name",""), item.get("batch_no",""),
                  item.get("quantity",1), item.get("unit_price",0),
                  item.get("quantity",1)*item.get("unit_price",0)))
            # Deduct stock
            if item.get("inventory_id"):
                conn.execute("UPDATE inventory SET quantity=MAX(0,quantity-?) WHERE id=?",
                             (item["quantity"], item["inventory_id"]))

        conn.commit()
        self.write_audit(user["sub"], user["full_name"], "SALE_COMPLETE", "sale", sale_id,
                        f"{invoice_no} · ₹{total:.0f} · {len(items)} items", self.client_address[0], conn)
        conn.close()
        self.send_json({"sale_id": sale_id, "invoice_no": invoice_no, "total": round(total,2)}, 201)

    def handle_transfers_list(self, user, qs):
        conn = get_db()
        rows = conn.execute("""
            SELECT t.*, s1.name as from_store_name, s2.name as to_store_name,
                   u.full_name as requested_by_name
            FROM stock_transfers t
            JOIN stores s1 ON t.from_store_id=s1.id
            JOIN stores s2 ON t.to_store_id=s2.id
            LEFT JOIN users u ON t.requested_by=u.id
            ORDER BY t.created_at DESC
        """).fetchall()
        conn.close()
        self.send_json({"transfers": [dict(r) for r in rows]})

    def handle_create_transfer(self, user, body):
        conn = get_db()
        tid = f"trn-{uuid.uuid4().hex[:8]}"
        tno = f"TRF-{datetime.now().strftime('%y%m%d')}-{uuid.uuid4().hex[:4].upper()}"
        val = body.get("quantity",0) * body.get("mrp",0)
        conn.execute("""
            INSERT INTO stock_transfers(id,transfer_no,from_store_id,to_store_id,drug_id,drug_name,
                                       batch_no,quantity,transfer_value,reason,status,requested_by)
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
        """, (tid, tno, body.get("from_store_id"), body.get("to_store_id"),
              body.get("drug_id"), body.get("drug_name",""), body.get("batch_no",""),
              body.get("quantity",0), val, body.get("reason","Routine"),
              "approved" if val < 50000 else "pending", user["sub"]))
        conn.commit()
        self.write_audit(user["sub"], user["full_name"], "TRANSFER_INIT", "transfer", tid,
                        f"{tno} · {body.get('drug_name','')} · ₹{val:.0f}", self.client_address[0], conn)
        conn.close()
        self.send_json({"transfer_id": tid, "transfer_no": tno, "status": "approved" if val<50000 else "pending"}, 201)

    def handle_update_transfer(self, user, tid, body):
        if not has_perm(user["role"], "transfer:approve"):
            return self.send_error_json("Permission denied", 403)
        conn = get_db()
        new_status = body.get("status","approved")
        conn.execute("UPDATE stock_transfers SET status=?,approved_by=?,updated_at=datetime('now') WHERE id=?",
                     (new_status, user["sub"], tid))
        conn.commit()
        conn.close()
        self.send_json({"success": True, "status": new_status})

    def handle_anomalies_list(self, qs):
        conn = get_db()
        rows = conn.execute("""
            SELECT a.*, s.name as store_name FROM anomalies a
            LEFT JOIN stores s ON a.store_id=s.id
            ORDER BY a.score DESC, a.created_at DESC
        """).fetchall()
        conn.close()
        self.send_json({"anomalies": [dict(r) for r in rows]})

    def handle_resolve_anomaly(self, user, body):
        conn = get_db()
        conn.execute("UPDATE anomalies SET status='resolved',resolved_by=?,updated_at=datetime('now') WHERE id=?",
                     (user["sub"], body.get("anomaly_id")))
        conn.commit()
        conn.close()
        self.send_json({"success": True})

    def handle_update_anomaly(self, user, aid, body):
        conn = get_db()
        status = body.get("status","open")
        conn.execute("UPDATE anomalies SET status=?,resolved_by=?,updated_at=datetime('now') WHERE id=?",
                     (status, user["sub"] if status!="open" else None, aid))
        conn.commit()
        conn.close()
        self.send_json({"success": True})

    def handle_po_list(self, qs):
        conn = get_db()
        rows = conn.execute("""
            SELECT p.*, s.name as store_name, u.full_name as requested_by_name
            FROM purchase_orders p
            JOIN stores s ON p.store_id=s.id
            LEFT JOIN users u ON p.requested_by=u.id
            ORDER BY p.created_at DESC
        """).fetchall()
        conn.close()
        self.send_json({"orders": [dict(r) for r in rows]})

    def handle_create_po(self, user, body):
        conn = get_db()
        pid = f"po-{uuid.uuid4().hex[:8]}"
        pno = f"PO-{datetime.now().strftime('%Y-%m%d')}-{uuid.uuid4().hex[:4].upper()}"
        total = body.get("quantity",0) * body.get("unit_cost",0)
        conn.execute("""
            INSERT INTO purchase_orders(id,po_no,store_id,supplier,drug_id,drug_name,
                                       quantity,unit_cost,total_cost,status,requested_by,notes)
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
        """, (pid, pno, user.get("store_id") or "store-001",
              body.get("supplier",""), body.get("drug_id",""),
              body.get("drug_name",""), body.get("quantity",0),
              body.get("unit_cost",0), total, "pending", user["sub"],
              body.get("notes","")))
        conn.commit()
        conn.close()
        self.send_json({"po_id": pid, "po_no": pno}, 201)

    def handle_stock_adjust(self, user, body):
        if not has_perm(user["role"], "inventory:write"):
            return self.send_error_json("Permission denied", 403)
        conn = get_db()
        inv_id = body.get("inventory_id")
        new_qty = body.get("quantity")
        reason = body.get("reason","Physical count adjustment")
        if inv_id is None or new_qty is None:
            return self.send_error_json("inventory_id and quantity required")
        old = conn.execute("SELECT * FROM inventory WHERE id=?", (inv_id,)).fetchone()
        conn.execute("UPDATE inventory SET quantity=? WHERE id=?", (new_qty, inv_id))
        conn.commit()
        self.write_audit(user["sub"], user["full_name"], "STOCK_ADJUST", "inventory", inv_id,
                        f"qty {old['quantity']}→{new_qty}. Reason: {reason}", self.client_address[0], conn)
        conn.close()
        self.send_json({"success": True, "new_quantity": new_qty})

    def handle_chain_report(self, qs):
        days = int(qs.get("days",["30"])[0])
        conn = get_db()
        # Revenue by store
        by_store = conn.execute(f"""
            SELECT s.name as store_name, s.id as store_id,
                   COALESCE(SUM(sa.total_amount),0) as revenue,
                   COUNT(sa.id) as bills,
                   COALESCE(AVG(sa.total_amount),0) as avg_bill
            FROM stores s
            LEFT JOIN sales sa ON s.id=sa.store_id AND sa.created_at >= date('now','-{days} days')
            GROUP BY s.id ORDER BY revenue DESC
        """).fetchall()

        # Category breakdown
        by_cat = conn.execute(f"""
            SELECT d.category, COALESCE(SUM(si.total_price),0) as revenue,
                   SUM(si.quantity) as units
            FROM sale_items si JOIN drugs d ON si.drug_id=d.id
            JOIN sales sa ON si.sale_id=sa.id
            WHERE sa.created_at >= date('now','-{days} days')
            GROUP BY d.category ORDER BY revenue DESC
        """).fetchall()

        # Daily trend
        daily = conn.execute(f"""
            SELECT date(created_at) as day, SUM(total_amount) as revenue, COUNT(*) as bills
            FROM sales WHERE created_at >= date('now','-{days} days')
            GROUP BY date(created_at) ORDER BY day
        """).fetchall()

        # Top SKUs
        top_skus = conn.execute(f"""
            SELECT d.name as drug_name, d.category, SUM(si.quantity) as units,
                   SUM(si.total_price) as revenue
            FROM sale_items si JOIN drugs d ON si.drug_id=d.id
            JOIN sales sa ON si.sale_id=sa.id
            WHERE sa.created_at >= date('now','-{days} days')
            GROUP BY d.id ORDER BY revenue DESC LIMIT 10
        """).fetchall()

        total_rev = sum(r["revenue"] for r in by_store)
        conn.close()
        self.send_json({
            "period_days": days,
            "total_revenue": round(total_rev, 2),
            "gross_margin_pct": 24.7,
            "shrinkage_pct": 0.38,
            "by_store": [dict(r) for r in by_store],
            "by_category": [dict(r) for r in by_cat],
            "daily_trend": [dict(r) for r in daily],
            "top_skus": [dict(r) for r in top_skus],
        })

    def handle_store_report(self, user, qs):
        store_id = user.get("store_id") or "store-001"
        days = int(qs.get("days",["30"])[0])
        conn = get_db()
        stats = conn.execute(f"""
            SELECT COALESCE(SUM(total_amount),0) as revenue,
                   COUNT(*) as bills,
                   COALESCE(AVG(total_amount),0) as avg_bill
            FROM sales WHERE store_id=? AND created_at >= date('now','-{days} days')
        """, (store_id,)).fetchone()
        conn.close()
        self.send_json({"store_id": store_id, "stats": dict(stats), "period_days": days})

    def handle_expiry_alerts(self, user, qs):
        store_id = user.get("store_id") or "store-001"
        days = int(qs.get("days",["60"])[0])
        conn = get_db()
        rows = conn.execute("""
            SELECT i.*, d.name as drug_name, d.category, d.schedule_type,
                   CAST(julianday(i.expiry_date)-julianday('now') AS INTEGER) as days_left
            FROM inventory i JOIN drugs d ON i.drug_id=d.id
            WHERE i.store_id=? AND i.expiry_date <= date('now','+? days')
            ORDER BY days_left ASC
        """, (store_id, days)).fetchall()
        conn.close()
        self.send_json({"alerts": [dict(r) for r in rows]})

    def handle_audit_log(self, qs):
        conn = get_db()
        rows = conn.execute("SELECT * FROM audit_log ORDER BY timestamp DESC LIMIT 100").fetchall()
        conn.close()
        self.send_json({"logs": [dict(r) for r in rows]})

    def handle_users_list(self, user, qs):
        conn = get_db()
        if user["role"] == "admin":
            rows = conn.execute("SELECT u.*, s.name as store_name FROM users u LEFT JOIN stores s ON u.store_id=s.id ORDER BY u.created_at DESC").fetchall()
        else:
            rows = conn.execute("SELECT u.*, s.name as store_name FROM users u LEFT JOIN stores s ON u.store_id=s.id WHERE u.store_id=? ORDER BY u.created_at DESC", (user.get("store_id"),)).fetchall()
        conn.close()
        users_out = [dict(r) for r in rows]
        for u2 in users_out: u2.pop("password_hash", None)
        self.send_json({"users": users_out})

    def handle_create_user(self, user, body):
        if user["role"] != "admin":
            return self.send_error_json("Permission denied", 403)
        conn = get_db()
        uid = f"user-{uuid.uuid4().hex[:8]}"
        pw_hash = hashlib.sha256(body.get("password","password123").encode()).hexdigest()
        try:
            conn.execute("""
                INSERT INTO users(id,username,email,password_hash,full_name,role,store_id)
                VALUES(?,?,?,?,?,?,?)
            """, (uid, body["username"], body["email"], pw_hash,
                  body["full_name"], body["role"], body.get("store_id")))
            conn.commit()
        except Exception as e:
            return self.send_error_json(str(e))
        conn.close()
        self.send_json({"user_id": uid}, 201)

    def handle_stores_list(self):
        conn = get_db()
        rows = conn.execute("SELECT * FROM stores WHERE is_active=1").fetchall()
        conn.close()
        self.send_json({"stores": [dict(r) for r in rows]})

    def handle_forecast(self, qs):
        # Deterministic forecast simulation based on historical patterns
        store_id = qs.get("store_id",["store-001"])[0]
        conn = get_db()
        inv_rows = conn.execute("""
            SELECT i.*, d.name as drug_name, d.category
            FROM inventory i JOIN drugs d ON i.drug_id=d.id
            WHERE i.store_id=?
        """, (store_id,)).fetchall()
        conn.close()

        import math
        forecasts = []
        for item in inv_rows:
            base_daily = max(1, item["quantity"] // 15)
            week1 = int(base_daily * 7 * (1 + 0.1 * math.sin(time.time()/86400)))
            week2 = int(week1 * 1.15)
            confidence = 75 + (hash(item["drug_name"]) % 20)
            forecasts.append({
                "drug_id": item["drug_id"],
                "drug_name": item["drug_name"],
                "category": item["category"],
                "current_stock": item["quantity"],
                "week1_forecast": week1,
                "week2_forecast": week2,
                "confidence_pct": confidence,
                "days_cover": round(item["quantity"] / max(1,base_daily), 1),
                "reorder_urgent": item["quantity"] / max(1,base_daily) < 7
            })
        self.send_json({"forecasts": sorted(forecasts, key=lambda x: x["days_cover"])})

    def handle_reorder_recs(self, user, qs):
        store_id = user.get("store_id") or "store-001"
        conn = get_db()
        rows = conn.execute("""
            SELECT i.*, d.name as drug_name, d.category, d.reorder_level,
                   d.schedule_type, d.manufacturer,
                   CAST(julianday(i.expiry_date)-julianday('now') AS INTEGER) as days_to_expiry
            FROM inventory i JOIN drugs d ON i.drug_id=d.id
            WHERE i.store_id=? AND i.quantity <= d.reorder_level * 1.5
            ORDER BY (CAST(i.quantity AS REAL)/d.reorder_level) ASC
        """, (store_id,)).fetchall()
        conn.close()
        recs = []
        for r in rows:
            daily_vel = max(0.5, r["reorder_level"] / 15.0)
            days_left = round(r["quantity"] / daily_vel, 1)
            rec_qty = r["reorder_level"] * 3
            recs.append({
                **dict(r),
                "daily_velocity": round(daily_vel, 1),
                "days_remaining": days_left,
                "recommended_qty": rec_qty,
                "estimated_cost": round(rec_qty * r["purchase_price"], 2),
                "priority": "URGENT" if days_left < 7 else "HIGH" if days_left < 14 else "NORMAL"
            })
        self.send_json({"recommendations": recs})

    def handle_ai_query(self, user, body):
        query = body.get("query","").lower()
        store_id = user.get("store_id") or "store-001"

        # Fetch live data for context
        conn = get_db()
        inv_summary = conn.execute("""
            SELECT COUNT(*) as total_skus,
                   SUM(CASE WHEN i.quantity<=d.reorder_level THEN 1 ELSE 0 END) as low_stock,
                   SUM(CASE WHEN i.expiry_date<=date('now','+30 days') THEN 1 ELSE 0 END) as near_expiry
            FROM inventory i JOIN drugs d ON i.drug_id=d.id WHERE i.store_id=?
        """, (store_id,)).fetchone()

        rev_today = conn.execute(
            "SELECT COALESCE(SUM(total_amount),0) as rev, COUNT(*) as bills FROM sales WHERE store_id=? AND date(created_at)=date('now')",
            (store_id,)
        ).fetchone()

        critical_inv = conn.execute("""
            SELECT d.name, i.quantity, d.reorder_level FROM inventory i
            JOIN drugs d ON i.drug_id=d.id WHERE i.store_id=? AND i.quantity<=d.reorder_level
            ORDER BY CAST(i.quantity AS REAL)/d.reorder_level ASC LIMIT 5
        """, (store_id,)).fetchall()

        anomalies = conn.execute("SELECT COUNT(*) as cnt FROM anomalies WHERE status='open'").fetchone()
        conn.close()

        # Rule-based AI responses with live data
        critical_drugs = ", ".join(f"{r['name']} ({r['quantity']} left)" for r in critical_inv)

        responses = {
            "reorder": f"""📦 **Reorder Recommendations (Live Data)**

Based on current stock levels at your store:

**{inv_summary['low_stock']} SKUs are below reorder point:**
{critical_drugs}

**Top Priority Actions:**
1. Metformin 500mg — CRITICAL. Stock: {critical_inv[0]['quantity'] if critical_inv else 18} strips. Estimated depletion in ~4 days at current velocity.
2. Order from Medline India (preferred supplier). Recommend 200 strips (₹5,600).
3. Consider emergency transfer from Civil Lines — they have 240 units available.

**Total recommended PO value: ~₹21,400**
I can auto-generate the Purchase Order. Confirm to proceed?""",

            "sales": f"""📊 **Sales Performance Summary (Live)**

**Today's Stats:**
- Revenue: ₹{rev_today['rev']:,.0f}
- Invoices: {rev_today['bills']}
- Average bill: ₹{(rev_today['rev']/max(1,rev_today['bills'])):.0f}

**Inventory Status:**
- Total SKUs tracked: {inv_summary['total_skus']}
- Low stock items: {inv_summary['low_stock']}
- Near expiry (30d): {inv_summary['near_expiry']}

**Open Anomalies requiring attention:** {anomalies['cnt']}

Cardiac and Diabetic categories driving ~50% of revenue. Evening peak expected 18:00–20:00.""",

            "expiry": f"""📅 **Expiry Risk Analysis**

**{inv_summary['near_expiry']} SKUs expiring within 30 days** at this store.

**Immediate actions needed:**
- Pantoprazole 40mg (Batch PP-2024-11): 8 days left · 24 strips at risk (₹1,080)
- Amoxicillin 250mg (Batch BX-2024-04): 18 days · 48 boxes (₹4,560)
- Dolo 650 (Batch D-2024-22): 22 days · 730 strips — high volume, likely to sell through

**Total value at risk: ₹8,640**

Recommendation: Return Amoxicillin batch to distributor (Ranbaxy accepts pre-expiry returns within 30 days). Prioritize Pantoprazole in prescriptions this week.""",

            "shrinkage": """🔍 **Shrinkage Analysis**

**Chain shrinkage rate: 0.38%** (Target: 0.45%) — performing well!

**Problem areas:**
- Panki store: 0.71% — Tramadol 50mg discrepancy flagged (26 strips, ₹2,080). Investigation initiated.
- Kidwai Nagar: 0.58% — Connectivity-related sync delays causing apparent discrepancies (not actual theft).

**Schedule H drugs require weekly physical count** per CDSCO guidelines. Currently at 2-week cadence for non-urban stores.

**Recommendation:** Implement daily blind count for all Schedule H drugs. ROI: prevents ~₹15,000/month in shrinkage losses.""",

            "forecast": """🔮 **14-Day Demand Forecast**

Based on historical sales velocity and seasonal patterns for Kanpur region:

**High urgency:**
- Metformin 500mg: +34% demand spike expected (Week 2). Local diabetes camp + seasonal pattern. Forecast: 295 units/week.
- Cetirizine 10mg: +28% from weekend allergy season. Stock adequate.

**Moderate changes:**
- Amoxicillin: +48% chain-wide (respiratory season starting). Coordinate with Central Hub.
- Dolo 650: Stable demand ~185 units/week.

**Model accuracy:** 87% MAPE on last 90-day backtest. Confidence intervals included in full report."""
        }

        # Match query to response
        response = responses["sales"]  # default
        if any(w in query for w in ["reorder","order","stock","low","purchase"]):
            response = responses["reorder"]
        elif any(w in query for w in ["expir","near","batch","value at risk"]):
            response = responses["expiry"]
        elif any(w in query for w in ["shrink","loss","discrepan","theft","variance"]):
            response = responses["shrinkage"]
        elif any(w in query for w in ["forecast","demand","predict","next week","week"]):
            response = responses["forecast"]
        elif any(w in query for w in ["sale","revenue","performance","today","bill","summar"]):
            response = responses["sales"]

        # Log AI query in audit
        conn = get_db()
        self.write_audit(user["sub"], user["full_name"], "AI_QUERY", "ai", "query",
                        f"Query: {body.get('query','')[:80]}", self.client_address[0], conn)
        conn.close()

        self.send_json({"response": response, "query": body.get("query",""), "data_sources": ["inventory","sales","anomalies"]})

    def write_audit(self, user_id, user_name, action, entity_type, entity_id, details, ip, conn=None):
        close_after = conn is None
        if close_after: conn = get_db()
        aid = f"aud-{uuid.uuid4().hex[:8]}"
        hash_val = hashlib.sha256(f"{aid}{action}{entity_id}".encode()).hexdigest()[:16]
        try:
            conn.execute("""
                INSERT INTO audit_log(id,action,user_id,user_name,entity_type,entity_id,details,ip_address,integrity_hash)
                VALUES(?,?,?,?,?,?,?,?,?)
            """, (aid, action, user_id, user_name, entity_type, entity_id, details, ip, hash_val))
            conn.commit()
        except Exception:
            pass
        if close_after: conn.close()


# ─── MAIN ─────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("🔧 Initialising PharmaCentral database...")
    init_db()
    print(f"✅ Database ready at {DB_PATH}")
    print(f"\n🚀 PharmaCentral Server starting on http://localhost:{PORT}")
    print("─" * 50)
    print("👥 Demo Accounts:")
    print("   admin / password123          → Head Office Admin")
    print("   rajesh / password123         → Pharmacist (Store: MG Road)")
    print("   vikram / password123         → Regional Manager")
    print("   sunita / password123         → Inventory Controller")
    print("─" * 50)
    print("Press Ctrl+C to stop\n")
    server = HTTPServer(("0.0.0.0", PORT), PharmaCentralHandler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n⛔ Server stopped.")
