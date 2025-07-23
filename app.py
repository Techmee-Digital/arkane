import os, uuid
from pathlib import Path
from datetime import datetime

import pandas as pd
from flask import (
    Flask, render_template, request, flash,
    redirect, url_for, send_from_directory
)
from flask_sqlalchemy import SQLAlchemy
from flask_login import (
    LoginManager, UserMixin,
    login_user, login_required,
    logout_user, current_user
)
from werkzeug.utils import secure_filename

BASE_DIR   = Path(__file__).parent
UPLOAD_DIR = BASE_DIR / "uploads"
UPLOAD_DIR.mkdir(exist_ok=True)

app = Flask(__name__)
app.config["SQLALCHEMY_DATABASE_URI"] = \
    "mysql+pymysql://root:Akeed@localhost/lead_lead_db"
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
app.config["SECRET_KEY"]                 = os.getenv("FLASK_SECRET","dev_secret")
app.config["UPLOAD_FOLDER"]              = UPLOAD_DIR

db  = SQLAlchemy(app)
lgm = LoginManager(app)
lgm.login_view = "login"

class Lead(db.Model):
    id          = db.Column(db.Integer, primary_key=True)
    email       = db.Column(db.String(255), index=True, unique=False, nullable=False)
    company     = db.Column(db.String(255))
    quarter     = db.Column(db.String(64))
    campaign    = db.Column(db.String(255))
    source_file = db.Column(db.String(255))
    upload_date = db.Column(db.DateTime, default=datetime.utcnow)

class User(UserMixin, db.Model):
    id       = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(64), unique=True, nullable=False)
    password = db.Column(db.String(128), nullable=False)

with app.app_context():
    db.create_all()
    if not User.query.filter_by(username="Campaign").first():
        db.session.add(User(username="Campaign", password="Arkane31"))
        db.session.commit()

@lgm.user_loader
def load_user(uid):
    return User.query.get(int(uid))

def normalize(s):
    return str(s).strip().lower()

def find_email_col(cols):
    for c in cols:
        if "mail" in c.lower():
            return c
    raise KeyError("No column containing “mail”; rename your email column.")

@app.route("/login", methods=["GET","POST"])
def login():
    if current_user.is_authenticated:
        return redirect(url_for("tools"))
    if request.method=="POST":
        u = request.form["username"]
        p = request.form["password"]
        user = User.query.filter_by(username=u, password=p).first()
        if user:
            login_user(user)
            return redirect(url_for("tools"))
        flash("Invalid credentials", "danger")
    return render_template("login.html")

@app.route("/logout")
@login_required
def logout():
    logout_user()
    return redirect(url_for("login"))

@app.route("/", methods=["GET","POST"])
@login_required
def tools():
    active_tab    = request.values.get("tab","duplicate")
    token         = request.values.get("token","")
    dedupe_ctx    = {
        "token":     "",
        "rows":      [],
        "fields":    [],
        "dup_set":   set(),
        "count_all": 0,
        "count_dup": 0,
        "sources":   ""
    }
    search_results = None
    merge_ctx      = {}
    all_db_leads   = Lead.query.order_by(Lead.upload_date.desc()).all()

    if request.method=="GET" and token:
        cache_path = UPLOAD_DIR/f"_cache_{token}.parquet"
        if cache_path.exists():
            big      = pd.read_parquet(cache_path)
            mask_dup = big.duplicated("Email", keep=False)
            existing = {
                e for (e,) in db.session
                    .query(Lead.email)
                    .filter(Lead.email.in_(big["Email"])).all()
            }
            dup_set = set(big.loc[mask_dup,"Email"]) | existing
            rows = []
            for _,r in big.iterrows():
                d = r.to_dict()
                is_dup = r["Email"] in dup_set
                d["duplicate"] = is_dup

        
                if is_dup:
                    if r["Email"] in existing:
                        lead = Lead.query.filter_by(email=r["Email"])\
                                        .order_by(Lead.upload_date.desc())\
                                        .first()
                        camp = lead.campaign or ""
                        qtr  = lead.quarter or ""
                        if camp or qtr:
                            d["origin"] = f"DB: {camp}/{qtr}"
                        else:
                            d["origin"] = "DB"
                    else:
                        d["origin"] = "Current Sheet"
                else:
                    d["origin"] = ""
                rows.append(d)

            dedupe_ctx = {
                "token":     token,
                "rows":      rows,
                "fields":    [c for c in big.columns if c!="__src__"] + ["__src__"],
                "dup_set":   dup_set,
                "count_all": len(rows),
                "count_dup": sum(1 for r in rows if r["duplicate"]),
                "sources":   ", ".join(big["__src__"].unique())
            }
    

    if request.method=="POST":
        action = request.form.get("action","")

    
        if action=="dedupe":
            files = request.files.getlist("file_upload")
            if not files or files[0].filename=="":
                flash("Select at least one Excel file", "warning")
                return redirect(url_for("tools", tab="duplicate"))

            token = uuid.uuid4().hex
            dfs, srcs = [], []
            for f in files:
                fn   = secure_filename(f.filename)
                path = UPLOAD_DIR/f"{token}_{fn}"
                f.save(path)
                df = pd.read_excel(path, engine="openpyxl")
                col = find_email_col(df.columns)
                df = df.rename(columns={col:"Email"})
                df["Email"]   = df["Email"].map(normalize)
                df["__src__"] = fn
                dfs.append(df)
                srcs.append(fn)

            big = pd.concat(dfs, ignore_index=True)
            mask_dup = big.duplicated("Email", keep=False)
            existing = {
                e for (e,) in db.session
                    .query(Lead.email)
                    .filter(Lead.email.in_(big["Email"])).all()
            }
            dup_set = set(big.loc[mask_dup,"Email"]) | existing

            rows = []
            for _,r in big.iterrows():
                d = r.to_dict()
                is_dup = r["Email"] in dup_set
                d["duplicate"] = is_dup

                
                if is_dup:
                    if r["Email"] in existing:
                        lead = Lead.query.filter_by(email=r["Email"])\
                                        .order_by(Lead.upload_date.desc())\
                                        .first()
                        camp = lead.campaign or ""
                        qtr  = lead.quarter or ""
                        if camp or qtr:
                            d["origin"] = f"DB: {camp}/{qtr}"
                        else:
                            d["origin"] = "DB"
                    else:
                        d["origin"] = "Current Sheet"
                else:
                    d["origin"] = ""
                rows.append(d)

            
            cache_path = UPLOAD_DIR/f"_cache_{token}.parquet"
            big.to_parquet(cache_path)

            dedupe_ctx = {
                "token":     token,
                "rows":      rows,
                "fields":    [c for c in big.columns if c!="__src__"] + ["__src__"],
                "dup_set":   dup_set,
                "count_all": len(rows),
                "count_dup": sum(1 for r in rows if r["duplicate"]),
                "sources":   ", ".join(srcs)
            }

       
        elif action in ("save_all","save_dup"):
            token = request.form["token"]
            path  = UPLOAD_DIR/f"_cache_{token}.parquet"
            if not path.exists():
                flash("No data to save; re-run check first", "warning")
                return redirect(url_for("tools", tab="duplicate"))
            df = pd.read_parquet(path)
            existing = {
                e for (e,) in db.session
                    .query(Lead.email)
                    .filter(Lead.email.in_(df["Email"])).all()
            }
            mask_dup = df.duplicated("Email", keep=False)
            dup_set  = set(df.loc[mask_dup,"Email"]) | existing

            saved = 0
            for _,r in df.iterrows():
                email = r["Email"]
                do = (action=="save_all") or (action=="save_dup" and email in dup_set)
                if do:
                    lead = Lead(
                        email       = email,
                        company     = r.get("Company",""),
                        quarter     = r.get("Quarter",""),
                        campaign    = r.get("Campaign",""),
                        source_file = r["__src__"]
                    )
                    db.session.add(lead)
                    saved += 1
            db.session.commit()
            flash(f"{saved} rows saved", "success")
            return redirect(url_for("tools", tab="duplicate"))

       
        elif action=="search":
            q = normalize(request.form["search_email"])
            search_results = Lead.query.filter_by(email=q).all()
            if not search_results:
                flash("No matches found", "info")
            active_tab = "search"

       
        elif action=="merge":
            files = request.files.getlist("file_merge")
            if not files or files[0].filename=="":
                flash("Select at least one Excel file", "warning")
                return redirect(url_for("tools", tab="merge"))

            dfs, hdrs = [], set()
            for f in files:
                df = pd.read_excel(f, engine="openpyxl")
                dfs.append(df)
                hdrs |= set(df.columns)
            hdrs = sorted(hdrs)
            merged = pd.concat(
                [df.reindex(columns=hdrs, fill_value="") for df in dfs],
                ignore_index=True
            )
            token = uuid.uuid4().hex
            out_fn = f"merged_{token}.xlsx"
            merged.to_excel(UPLOAD_DIR/out_fn, index=False, engine="openpyxl")
            merge_ctx = {
                "headers": hdrs,
                "records": merged.to_dict("records"),
                "download": out_fn
            }
            active_tab = "merge"

    return render_template(
        "tools.html",
        active_tab=active_tab,
        dedupe=dedupe_ctx,
        search_results=search_results,
        merge=merge_ctx,
        viewdb=all_db_leads
    )

@app.route("/uploads/<path:fn>")
@login_required
def uploaded_file(fn):
    return send_from_directory(UPLOAD_DIR, fn)

if __name__=="__main__":
    app.run(debug=True)
