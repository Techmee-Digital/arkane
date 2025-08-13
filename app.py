import os
import uuid
from pathlib import Path
from datetime import datetime

import pandas as pd
from flask import (
    Flask, render_template, request, flash,
    redirect, url_for, send_from_directory
)
from werkzeug.utils import secure_filename
from flask_migrate import Migrate
from flask_login import (
    LoginManager, login_user,
    login_required, logout_user,
    current_user
)
from dotenv import load_dotenv

from model import db, Lead, User

load_dotenv()


def create_app():
    app = Flask(__name__)

    # ─── SECRET KEY ─────────────────────────────────────────────────────────
    secret = os.getenv('FLASK_SECRET_KEY')
    if not secret:
        raise RuntimeError("FLASK_SECRET_KEY must be set in the environment")
    app.config['SECRET_KEY'] = secret
    app.secret_key = secret

    # ─── DATABASE & OTHER CONFIG ─────────────────────────────────────────────
    app.config['SQLALCHEMY_DATABASE_URI'] = os.getenv('DATABASE_URL')
    app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
    app.config['UPLOAD_FOLDER'] = os.getenv(
        'UPLOAD_FOLDER',
        str(Path(__file__).parent / 'uploads')
    )
    app.config['MAX_CONTENT_LENGTH'] = int(
        os.getenv('MAX_CONTENT_MB', 16)) * 1024 * 1024
    app.config['ALLOWED_EXTENSIONS'] = set(
        os.getenv('ALLOWED_EXT', 'xls,xlsx').split(',')
    )

    # ensure upload folder exists
    Path(app.config['UPLOAD_FOLDER']).mkdir(parents=True, exist_ok=True)

    # ─── Initialize extensions ──────────────────────────────────────────────
    db.init_app(app)
    Migrate(app, db)

    # …and then the rest of your login_manager & route definitions…

    login_manager = LoginManager()
    login_manager.init_app(app)
    login_manager.login_view = 'login'

    @login_manager.user_loader
    def load_user(user_id):
        return db.session.get(User, int(user_id))

    def allowed_file(filename: str) -> bool:
        return '.' in filename and filename.rsplit(
    '.', 1)[1].lower() in app.config['ALLOWED_EXTENSIONS']

    def normalize(s):
        return str(s).strip().lower()

    def find_email_col(cols):
        for c in cols:
            if 'mail' in c.lower():
                return c
        raise KeyError(
            "No column containing 'mail'; rename your email column.")

    @app.errorhandler(413)
    def file_too_large(e):
        flash(
            f"File too large (max {app.config['MAX_CONTENT_LENGTH'] // (1024 * 1024)} MB)", "danger")
        return redirect(request.url)

    @app.route('/login', methods=['GET', 'POST'])
    def login():
        if current_user.is_authenticated:
            return redirect(url_for('tools'))
        if request.method == 'POST':
            username = request.form.get('username')
            password = request.form.get('password')
            user = User.query.filter_by(username=username).first()
            if user and user.check_password(password):
                login_user(user)
                return redirect(url_for('tools'))
            flash("Invalid credentials", "danger")
        return render_template('login.html')

    @app.route('/logout')
    @login_required
    def logout():
        logout_user()
        return redirect(url_for('login'))

    @app.route('/', methods=['GET', 'POST'])
    @login_required
    def tools():
        active_tab = request.values.get('tab', 'duplicate')
        token = request.values.get('token', '')
        dedupe_ctx = {"token": "", "rows": [], "fields": [], "dup_set": set(),
                      "count_all": 0, "count_dup": 0, "sources": ""}
        search_results = None
        merge_ctx = {}

        if request.method == 'GET' and token:
            cache_path = Path(
    app.config['UPLOAD_FOLDER']) / f"_cache_{token}.parquet"
            if cache_path.exists():
                big = pd.read_parquet(cache_path)
                big.columns = [c.lower() for c in big.columns]
                mask_dup = big.duplicated("email", keep=False)
                existing = {
                    e for (e,) in db.session
                    .query(Lead.email)
                    .filter(Lead.email.in_(big["email"])).all()
                }
                dup_set = set(big.loc[mask_dup, "email"]) | existing
                rows = []
                for _, r in big.iterrows():
                    d = r.to_dict()
                    is_dup = r["email"] in dup_set
                    d["duplicate"] = is_dup
                    if is_dup:
                        if r["email"] in existing:
                            lead = Lead.query.filter_by(email=r["email"])\
                                              .order_by(Lead.upload_date.desc()).first()
                            camp = lead.campaign or ""
                            qtr = lead.quarter or ""
                            d["origin"] = f"DB: {camp}/{qtr}" if (
                                camp or qtr) else "DB"
                        else:
                            d["origin"] = "Current Sheet"
                    else:
                        d["origin"] = ""
                    rows.append(d)

                dedupe_ctx = {
                    "token": token,
                    "rows": rows,
                    "fields": [c for c in big.columns if c != "__src__"] + ["__src__"],
                    "dup_set": dup_set,
                    "count_all": len(rows),
                    "count_dup": sum(1 for r in rows if r["duplicate"]),
                    "sources": ", ".join(big["__src__"].unique())
                }

        if request.method == 'POST':
            action = request.form.get('action', '')

            if action == "dedupe":
                files = request.files.getlist("file_upload")
                if not files or not files[0].filename:
                    flash("Select at least one Excel file", "warning")
                    return redirect(url_for("tools", tab="duplicate"))

                token = uuid.uuid4().hex
                dfs, srcs = [], []
                for f in files:
                    if not allowed_file(f.filename):
                        flash(
    f"Unsupported file type: {
        f.filename}", "warning")
                        continue
                    fn = secure_filename(f.filename)
                    path = Path(app.config['UPLOAD_FOLDER']) / f"{token}_{fn}"
                    f.save(path)

                    df = pd.read_excel(path, engine="openpyxl")
                    col = find_email_col(df.columns)
                    df = df.rename(columns={col: "Email"})
                    df["Email"] = df["Email"].map(normalize)
                    df["__src__"] = fn

                    df.columns = [str(c).strip().lower() for c in df.columns]

                    if "campaign name" in df.columns:
                        df.rename(
    columns={
        "campaign name": "campaign"},
         inplace=True)

                    if "exclusions" not in df.columns:
                       df["exclusions"] = ""

                    dfs.append(df)
                    srcs.append(fn)

                if not dfs:
                    flash("No valid Excel files uploaded", "warning")
                    return redirect(url_for("tools", tab="duplicate"))

                big = pd.concat(dfs, ignore_index=True)
                mask_dup = big.duplicated("email", keep=False)
                existing = {
                    e for (e,) in db.session.query(Lead.email)
                    .filter(Lead.email.in_(big["email"])).all()
                }
                dup_set = set(big.loc[mask_dup, "email"]) | existing

                rows = []
                for _, r in big.iterrows():
                    d = r.to_dict()
                    is_dup = r["email"] in dup_set
                    d["duplicate"] = is_dup
                    if is_dup:
                        if r["email"] in existing:
                            lead = Lead.query.filter_by(email=r["email"])\
                                              .order_by(Lead.upload_date.desc()).first()
                            camp = lead.campaign or ""
                            qtr = lead.quarter or ""
                            d["origin"] = f"DB: {camp}/{qtr}" if (
                                camp or qtr) else "DB"
                        else:
                            d["origin"] = "Current Sheet"
                    else:
                        d["origin"] = ""
                    rows.append(d)

                cache_path = Path(
    app.config['UPLOAD_FOLDER']) / f"_cache_{token}.parquet"
                big.to_parquet(cache_path)

                dedupe_ctx = {
                    "token": token,
                    "rows": rows,
                    "fields": [c for c in big.columns if c != "__src__"] + ["__src__"],
                    "dup_set": dup_set,
                    "count_all": len(rows),
                    "count_dup": sum(1 for r in rows if r["duplicate"]),
                    "sources": ", ".join(srcs)
                }

            elif action in ("save_all", "save_dup"):
                token = request.form.get("token", "")
                cache_path = Path(app.config['UPLOAD_FOLDER']) / f"_cache_{token}.parquet"

                if not cache_path.exists():
                    flash("No data to save; re-run check first", "warning")
                    return redirect(url_for("tools", tab="duplicate"))

                # Load cached DataFrame and normalize column names
                df = pd.read_parquet(cache_path)
                df.columns = [c.lower() for c in df.columns]

                # Alias “campaign name” → “campaign”
                if "campaign name" in df.columns:
                    df.rename(columns={"campaign name": "campaign"}, inplace=True)
                # Ensure an exclusions column always exists
                if "exclusions" not in df.columns:
                    df["exclusions"] = ""

                # Figure out which emails are duplicates or existing
                existing = {
                    e for (e,) in db.session.query(Lead.email)
                    .filter(Lead.email.in_(df["email"])).all()
                }
                mask_dup = df.duplicated("email", keep=False)
                dup_set = set(df.loc[mask_dup, "email"]) | existing

                # Persist rows to the database
                saved = 0
                for _, r in df.iterrows():
                    email = r["email"]
                    do = (action == "save_all") or (email in dup_set)
                    if not do:
                        continue

                    lead = Lead(
                        email=email,
                        company=r.get("company", ""),
                        quarter=r.get("quarter", ""),
                        campaign=r.get("campaign", ""),
                        source_file=r.get("__src__", ""),
                        exclusions=r.get("exclusions", "")
                    )
                    db.session.add(lead)
                    saved += 1

                db.session.commit()
                flash(f"{saved} rows saved", "success")
                return redirect(url_for("tools", tab="duplicate"))

            elif action == "search":
                q = normalize(request.form.get("search_email", ""))
                search_results = Lead.query.filter_by(email=q).all()
                if not search_results:
                    flash("No matches found", "info")
                active_tab = "search"


            elif action == "merge":
                files = request.files.getlist("file_merge")
                if not files or not files[0].filename:
                    flash("Select at least one Excel file", "warning")
                    return redirect(url_for("tools", tab="merge"))

                dfs, hdrs = [], set()
                for f in files:
                    if not allowed_file(f.filename):
                        flash(f"Unsupported file type: {f.filename}", "warning")
                        continue
                    df = pd.read_excel(f, engine="openpyxl")
                    dfs.append(df)
                    hdrs |= set(df.columns)

                if not dfs:
                    flash("No valid Excel files uploaded", "warning")
                    return redirect(url_for("tools", tab="merge"))

                hdrs = sorted(hdrs)
                merged = pd.concat([
                    df.reindex(columns=hdrs, fill_value="") for df in dfs
                ], ignore_index=True)

                token = uuid.uuid4().hex
                out_fn = f"merged_{token}.xlsx"
                merged.to_excel(Path(app.config['UPLOAD_FOLDER']) / out_fn,
                                index=False, engine="openpyxl")

                merge_ctx = {
                    "headers": hdrs,
                    "records": merged.to_dict("records"),
                    "download": out_fn
                }
                active_tab = "merge"
                    # ─── Viewdb with checkbox-controlled filters ────────────────────────
            if active_tab == 'viewdb':
            # only read the search terms if their box was checked
                enable_email   = request.values.get('enable_email')   == '1'
                enable_company = request.values.get('enable_company') == '1'

            # normalize inputs (empty if not enabled)
            email_q   = normalize(request.values.get('filter_email', ''))   if enable_email   else ''
            company_q = normalize(request.values.get('filter_company', '')) if enable_company else ''

            query = Lead.query
            if enable_email and email_q:
                query = query.filter(Lead.email.ilike(f"%{email_q}%"))
            if enable_company and company_q:
                query = query.filter(Lead.company.ilike(f"%{company_q}%"))

            all_db_leads = query.order_by(Lead.upload_date.desc()).all()
        else:
            # fallback on other tabs
            all_db_leads = Lead.query.order_by(Lead.upload_date.desc()).all()


        return render_template(
            "tools.html",
            active_tab=active_tab,
            dedupe=dedupe_ctx,
            search_results=search_results,
            merge=merge_ctx,
            viewdb=all_db_leads
        )

    @app.route("/uploads/<path:filename>")
    @login_required
    def uploaded_file(filename):
        return send_from_directory(app.config['UPLOAD_FOLDER'], filename)

    def create_default_user():
        admin_user = os.getenv('ADMIN_USER', 'Campaign')
        admin_pw = os.getenv('ADMIN_PASSWORD', 'Arkane31')
        if not User.query.filter_by(username=admin_user).first():
            u = User(username=admin_user)
            u.set_password(admin_pw)
            db.session.add(u)
            db.session.commit()
            app.logger.info(f"Created default user '{admin_user}'")

    with app.app_context():
        db.create_all()
        create_default_user()

    return app

if __name__ == "__main__":
    application = create_app()
    application.run(
        host="0.0.0.0",
        port=int(os.getenv("PORT", 5000)),
        debug=False
    )
