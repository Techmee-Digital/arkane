import os
import uuid
import io
from pathlib import Path
import pandas as pd
from flask import (
    Flask, render_template, request, flash,
    redirect, url_for, send_from_directory, current_app, send_file
)
from werkzeug.utils import secure_filename
from flask_migrate import Migrate
from flask_login import (
    LoginManager, login_user,
    login_required, logout_user,
    current_user
)
from dotenv import load_dotenv

from model import db, Lead, User, Team
import sqlalchemy as sa
from sqlalchemy import inspect
from sqlalchemy.exc import SQLAlchemyError



def table_has_column(engine, table, column) -> bool:
    insp = inspect(engine)
    try:
        return any(c['name'] == column for c in insp.get_columns(table))
    except SQLAlchemyError:
        return False
def ensure_min_schema_and_seed():
    """
    Idempotent bootstrap for MariaDB:
    - Add missing columns (users.team_id, users.role, leads.team_id)
    - Add helpful indexes
    - Create/ensure base team -> backfill NULL team_id
    - Make team_id NOT NULL
    - Best-effort add FKs
    - Seed default teams/users
    """
    engine = db.engine

    # 1) Add missing columns/indexes (start as NULL so we can backfill safely)
    with engine.begin() as conn:
        # users.team_id
        if not table_has_column(engine, 'users', 'team_id'):
            conn.execute(sa.text("ALTER TABLE users ADD COLUMN team_id INT NULL"))
            try:
                conn.execute(sa.text("CREATE INDEX ix_users_team_id ON users (team_id)"))
            except Exception:
                pass

        # users.role
        if not table_has_column(engine, 'users', 'role'):
            conn.execute(sa.text("ALTER TABLE users ADD COLUMN role VARCHAR(32) DEFAULT 'member'"))

        # leads.team_id
        if not table_has_column(engine, 'leads', 'team_id'):
            conn.execute(sa.text("ALTER TABLE leads ADD COLUMN team_id INT NULL"))
            try:
                conn.execute(sa.text("CREATE INDEX ix_leads_team_id ON leads (team_id)"))
            except Exception:
                pass

        # composite index (team_id,email) for fast per-team lookups
        try:
            conn.execute(sa.text("CREATE INDEX ix_leads_team_email ON leads (team_id, email)"))
        except Exception:
            pass

    # 2) Ensure a base team so we can backfill safely
    campaign = Team.query.filter_by(name="Campaign").first()
    if not campaign:
        campaign = Team(name="Campaign")
        db.session.add(campaign)
        db.session.commit()

    # 3) Backfill any NULL team_id to Campaign
    with engine.begin() as conn:
        conn.execute(sa.text("UPDATE users SET team_id = :tid WHERE team_id IS NULL"), {"tid": campaign.id})
        conn.execute(sa.text("UPDATE leads SET team_id = :tid WHERE team_id IS NULL"), {"tid": campaign.id})

    # 4) Make columns NOT NULL now that they’re populated
    with engine.begin() as conn:
        conn.execute(sa.text("ALTER TABLE users MODIFY team_id INT NOT NULL"))
        conn.execute(sa.text("ALTER TABLE leads MODIFY team_id INT NOT NULL"))

        # 5) Best-effort FKs (ignore if they already exist / different names)
        try:
            conn.execute(sa.text("""
                ALTER TABLE users
                ADD CONSTRAINT fk_users_team
                FOREIGN KEY (team_id) REFERENCES teams(id)
                ON UPDATE CASCADE ON DELETE RESTRICT
            """))
        except Exception:
            pass

        try:
            conn.execute(sa.text("""
                ALTER TABLE leads
                ADD CONSTRAINT fk_leads_team
                FOREIGN KEY (team_id) REFERENCES teams(id)
                ON UPDATE CASCADE ON DELETE RESTRICT
            """))
        except Exception:
            pass

    # 6) Seed remaining teams/users (idempotent)
    create_default_users()


# put this near the top of app.py, after imports
def create_default_users():
    # Ensure teams
    campaign_team = Team.query.filter_by(name="Campaign").first()
    if not campaign_team:
        campaign_team = Team(name="Campaign")
        db.session.add(campaign_team)

    lead_team = Team.query.filter_by(name="Leadgen").first()
    if not lead_team:
        lead_team = Team(name="Leadgen")
        db.session.add(lead_team)

    email_team = Team.query.filter_by(name="Email Marketing").first()
    if not email_team:
        email_team = Team(name="Email Marketing")
        db.session.add(email_team)

    db.session.commit()

    # Campaign user (from env or defaults)
    admin_user = os.getenv('ADMIN_USER', 'Campaign')
    admin_pw = os.getenv('ADMIN_PASSWORD', 'Arkane31')
    u = User.query.filter_by(username=admin_user).first()
    if not u:
        u = User(username=admin_user, team_id=campaign_team.id)
        u.set_password(admin_pw)
        db.session.add(u)
    elif u.team_id is None:
        u.team_id = campaign_team.id

    # Leadgen user
    u = User.query.filter_by(username="lead").first()
    if not u:
        u = User(username="lead", team_id=lead_team.id)
        u.set_password("lead12")
        db.session.add(u)
    elif u.team_id is None:
        u.team_id = lead_team.id

    # Email Marketing user
    u = User.query.filter_by(username="email").first()
    if not u:
        u = User(username="email", team_id=email_team.id)
        u.set_password("email12")
        db.session.add(u)
    elif u.team_id is None:
        u.team_id = email_team.id

    db.session.commit()


load_dotenv()
 # --- Simple pagination helper (mimics Flask-SQLAlchemy paginate bits) ---
class SimplePagination:
    def __init__(self, page, per_page, total, items):
        self.page = page
        self.per_page = per_page
        self.total = total
        self.items = items

    @property
    def has_prev(self):
        return self.page > 1

    @property
    def has_next(self):
        return self.page * self.per_page < self.total

    @property
    def prev_num(self):
        return self.page - 1

    @property
    def next_num(self):
        return self.page + 1

    def iter_pages(self, left_edge=1, right_edge=1, left_current=2, right_current=2):
        last = 0
        total_pages = (self.total + self.per_page - 1) // self.per_page
        for num in range(1, total_pages + 1):
            if (
                num <= left_edge
                or (num >= self.page - left_current and num <= self.page + right_current)
                or num > total_pages - right_edge
            ):
                if last + 1 != num:
                    yield None
                yield num
                last = num


def create_app():
    app = Flask(__name__)
    app.jinja_env.add_extension('jinja2.ext.do') # <-- ADD THIS LINE


    # ─── SECRET KEY ─────────────────────────────────────────────────────────
    secret = os.getenv('FLASK_SECRET_KEY')
    if not secret:
        raise RuntimeError("FLASK_SECRET_KEY must be set in the environment")
    app.config['SECRET_KEY'] = secret
    app.secret_key = secret

    # ─── DATABASE & OTHER CONFIG ─────────────────────────────────────
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
    app.config['ADMIN_ACTION_PASSWORD'] = os.getenv('ADMIN_ACTION_PASSWORD', 'Cricket12')

    # ensure upload folder exists
    Path(app.config['UPLOAD_FOLDER']).mkdir(parents=True, exist_ok=True)

    # ─── Initialize extensions ──────────────────────────────────────
    db.init_app(app)
    Migrate(app, db)

    # ─── Login manager setup ─────────────────────────────────────────
    login_manager = LoginManager()
    login_manager.init_app(app)
    login_manager.login_view = 'login'
    login_manager.login_message = None  # 

    @login_manager.user_loader
    def load_user(user_id):
        return db.session.get(User, int(user_id))

    def allowed_file(filename: str) -> bool:
        return (
            '.' in filename
            and filename.rsplit('.', 1)[1].lower() in app.config['ALLOWED_EXTENSIONS']
        )

    def find_email_col(cols):
        for c in cols:
            if 'mail' in c.lower():
                return c
        raise KeyError("No column containing 'mail'; rename your email column.")

    @app.errorhandler(413)
    def file_too_large(e):
        # Friendlier error; most often happens when posting every input on big tables
        flash(
            f"Request too large. Tip: for actions use the buttons which now submit only the selected rows. "
            f"(Max {app.config['MAX_CONTENT_LENGTH'] // (1024 * 1024)} MB).",
            "danger"
        )
        return redirect(url_for('tools', tab='viewdb'))

    # ─── Login routes ──────────────────────────────────────────────
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

    # ─── Main tool route ───────────────────────────────────────────
    @app.route('/', methods=['GET', 'POST'])
    @login_required
    def tools():
        active_tab = request.values.get('tab', 'duplicate')
        token = request.values.get('token', '')

        # default contexts
        dedupe_ctx = {"token": "", "rows": [], "fields": [], "dup_set": set(),
                      "count_all": 0, "count_dup": 0, "sources": ""}
        dedupe_pagination = None  # <--- add this
        search_results = None
        merge_ctx = {}

        # ---------- GET (token refresh for Duplicate Checker) ----------
        if request.method == 'GET' and token:
            cache_path = Path(current_app.config['UPLOAD_FOLDER']) / f"_cache_{token}.parquet"
            if cache_path.exists():
                big = pd.read_parquet(cache_path)
                big.columns = [c.lower() for c in big.columns]
                mask_dup = big.duplicated("email", keep=False)
                existing = {
                e for (e,) in db.session.query(Lead.email)
                .filter(Lead.team_id == current_user.team_id,
                        Lead.email.in_(big["email"])).all()
            }

                dup_set = set(big.loc[mask_dup, "email"]) | existing

                rows = []
                for _, r in big.iterrows():
                    d = r.to_dict()
                    is_dup = r["email"] in dup_set
                    d["duplicate"] = is_dup
                    if is_dup:
                        if r["email"] in existing:
                            lead = (Lead.query.filter_by(email=r["email"], team_id=current_user.team_id)
                            .order_by(Lead.upload_date.desc()).first())

                            camp = lead.campaign or ""
                            qtr = lead.quarter or ""
                            d["origin"] = f"DB: {camp}/{qtr}" if (camp or qtr) else "DB"
                        else:
                            d["origin"] = "Current Sheet"
                    else:
                        d["origin"] = "Current Sheet"

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
                # ---- DUPLICATE CHECKER PAGINATION (GET with token) ----
        dpage = request.args.get('dpage', 1, type=int)
        dper_page = 100  # adjust if you want different page size
        dtotal = len(dedupe_ctx["rows"])
        start = (dpage - 1) * dper_page
        end = start + dper_page
        page_items = dedupe_ctx["rows"][start:end]

        dedupe_pagination = SimplePagination(
            page=dpage, per_page=dper_page, total=dtotal, items=page_items
        )

        # replace rows shown in this request with just the current page
        dedupe_ctx["rows"] = page_items


        # ---------- POST (all actions) ----------
        if request.method == 'POST':
            action = request.form.get('action', '')
            admin_pass = request.form.get("admin_pass", "")

            # ===== Update Selected (inline) =====
            if action == "update_selected":
                if admin_pass != current_app.config['ADMIN_ACTION_PASSWORD']:
                    flash("Invalid Admin Password.", "danger")
                    return redirect(url_for("tools", tab="viewdb"))

                selected_ids = [int(x) for x in request.form.getlist('lead_ids')]
                if not selected_ids:
                    flash('No leads were selected to update.', 'warning')
                    return redirect(url_for("tools", tab="viewdb"))

                updated_count = 0
                for lead_id in selected_ids:
                    lead = db.session.get(Lead, lead_id)
                    if not lead or lead.team_id != current_user.team_id: 
                        continue
                    lead.email       = (request.form.get(f"email_{lead_id}", lead.email) or "").strip().lower()
                    lead.company     = request.form.get(f"company_{lead_id}", lead.company) or ""
                    lead.quarter     = request.form.get(f"quarter_{lead_id}", lead.quarter) or ""
                    lead.campaign    = request.form.get(f"campaign_{lead_id}", lead.campaign) or ""
                    # optional fields if present in form
                    if f"source_file_{lead_id}" in request.form:
                        lead.source_file = request.form.get(f"source_file_{lead_id}", lead.source_file) or ""
                    if f"exclusions_{lead_id}" in request.form:
                        lead.exclusions = request.form.get(f"exclusions_{lead_id}", lead.exclusions) or ""
                    updated_count += 1

                if updated_count:
                    db.session.commit()
                    flash(f"Successfully updated {updated_count} lead(s).", "success")

                return redirect(url_for("tools", tab="viewdb"))

            # ===== Delete Selected — delete ONLY the checked row(s) by ID =====
            elif action == "delete_selected":
                if admin_pass != current_app.config['ADMIN_ACTION_PASSWORD']:
                    flash("Invalid Admin Password.", "danger")
                    return redirect(url_for("tools", tab="viewdb"))

                selected_ids = [int(x) for x in request.form.getlist('lead_ids')]
                if not selected_ids:
                    flash('No leads were selected to delete.', 'warning')
                    return redirect(url_for("tools", tab="viewdb"))

                deleted = Lead.query.filter(
                    Lead.id.in_(selected_ids),
                    Lead.team_id == current_user.team_id
                ).delete(synchronize_session=False)
                db.session.commit()
                flash(f"{deleted} selected record(s) deleted.", "success")
                return redirect(url_for("tools", tab="viewdb"))

            # ===== Delete all from Source (when filtered by source) =====
            elif action == "delete_source_results":
                if admin_pass != current_app.config['ADMIN_ACTION_PASSWORD']:
                    flash("Invalid Admin Password.", "danger")
                    return redirect(url_for("tools", tab="viewdb"))

                src = request.form.get("source_to_delete", "")
                if not src:
                    flash("Source name not found.", "warning")
                else:
                    deleted = Lead.query.filter(
                        Lead.source_file == src,
                        Lead.team_id == current_user.team_id
                    ).delete(synchronize_session=False)

                    db.session.commit()
                    flash(f"Deleted {deleted} lead(s) from source '{src}'", "success")
                return redirect(url_for("tools", tab="viewdb"))

            # ===== Download actions (no password required) =====
            elif action in ['download_selected', 'download_all', 'download_filtered']:
                try:   
                    print("DOWNLOAD FORM DATA:", dict(request.form)) 

                    query = Lead.query.filter(Lead.team_id == current_user.team_id)
                    filename = "leads.xlsx"
              
                    if action == 'download_all':
                        filename = "all_leads.xlsx"

                    elif action == 'download_selected':
                        selected_ids = [int(x) for x in request.form.getlist('lead_ids')]
                        if not selected_ids:
                            flash('No leads selected to download.', 'warning')
                            return redirect(url_for('tools', tab='viewdb'))
                        query = query.filter(Lead.id.in_(selected_ids))
                        filename = "selected_leads.xlsx"

                    elif action == 'download_filtered':
                        if request.form.get('enable_email') and request.form.get('filter_email'):
                            query = query.filter(Lead.email.ilike(f"%{request.form.get('filter_email')}%"))
                        if request.form.get('enable_campaign') and request.form.get('filter_campaign'):
                            query = query.filter(Lead.campaign.ilike(f"%{request.form.get('filter_campaign')}%"))
                        if request.form.get('enable_company') and request.form.get('filter_company'):
                            query = query.filter(Lead.company.ilike(f"%{request.form.get('filter_company')}%"))
                        if request.form.get('enable_source') and request.form.get('filter_source'):
                            query = query.filter(Lead.source_file.ilike(f"%{request.form.get('filter_source')}%"))
                        filename = "filtered_leads.xlsx"


                    leads = query.order_by(Lead.upload_date.desc()).all()
                    if not leads:
                        flash('No data found to download for the given criteria.', 'warning')
                        return redirect(url_for('tools', tab='viewdb'))

                    leads_data = [
                        {c.name: getattr(lead, c.name) for c in lead.__table__.columns}
                        for lead in leads
                    ]
                    df = pd.DataFrame(leads_data)
                    output = io.BytesIO()
                    with pd.ExcelWriter(output, engine='openpyxl') as writer:
                        df.to_excel(writer, index=False, sheet_name='Leads')
                    output.seek(0)

                    return send_file(
                        output,
                        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                        as_attachment=True,
                        download_name=filename
                    )
                except ImportError:
                    flash("Error: install dependencies: pip install pandas openpyxl", "danger")
                    return redirect(url_for('tools', tab='viewdb'))
                except Exception as e:
                    flash(f"Unexpected error during download: {e}", "danger")
                    return redirect(url_for('tools', tab='viewdb'))

            # ===== Dedupe upload =====
             # ===== Dedupe upload =====
            elif action == "dedupe":
                files = request.files.getlist("file_upload")
                if not files or not files[0].filename:
                    flash("Select at least one Excel file", "warning")
                    return redirect(url_for("tools", tab="duplicate"))

                token = uuid.uuid4().hex
                dfs, srcs = [], []
                # START: This loop should ONLY read files and add them to the 'dfs' list
                for f in files:
                    if not allowed_file(f.filename):
                        flash(f"Unsupported file type: {f.filename}", "warning")
                        continue

                    fn = secure_filename(f.filename)
                    # Note: You can save the file directly from memory without saving to disk first
                    # but for simplicity, we'll keep your original logic.
                    path = Path(current_app.config['UPLOAD_FOLDER']) / f"{token}_{fn}"
                    f.save(path)

                    df = pd.read_excel(path, engine="openpyxl")

                    # Normalize email column
                    try:
                        col = find_email_col(df.columns)
                    except KeyError as e:
                        flash(f"Error in file '{fn}': {e}", "danger")
                        continue # Skip this file and proceed to the next

                    df = df.rename(columns={col: "Email"})
                    df["Email"] = df["Email"].map(lambda s: str(s).strip().lower())

                    # Source marker
                    df["__src__"] = fn

                    # Standardize headers
                    df.columns = [str(c).strip().lower() for c in df.columns]
                    if "campaign name" in df.columns:
                        df.rename(columns={"campaign name": "campaign"}, inplace=True)
                    if "exclusions" not in df.columns:
                        df["exclusions"] = ""

                    # Replace NaN with empty strings for string-y columns
                    for colname in ["company", "quarter", "campaign", "__src__", "exclusions"]:
                        if colname in df.columns:
                            df[colname] = df[colname].where(pd.notnull(df[colname]), "")

                    dfs.append(df)
                    srcs.append(fn)
                # END: The for loop is now finished.

                # <<< FIX: THE FOLLOWING LOGIC IS NOW DE-INDENTED >>>
                # It runs once *after* all files have been read into the 'dfs' list.
                if not dfs:
                    flash("No valid Excel files were processed.", "warning")
                    return redirect(url_for("tools", tab="duplicate"))

                big = pd.concat(dfs, ignore_index=True)
                mask_dup = big.duplicated("email", keep=False)
                # Get all leads from the DB that match emails in the uploaded file
                matching_leads = Lead.query.filter(
                    Lead.team_id == current_user.team_id,
                    Lead.email.in_(big["email"])
                ).order_by(Lead.email, Lead.upload_date.desc()).all()

                # Create a map of email -> latest lead object
                existing_leads_map = {}
                for lead in matching_leads:
                    if lead.email not in existing_leads_map:
                        existing_leads_map[lead.email] = lead

                existing = set(existing_leads_map.keys())

                dup_set = set(big.loc[mask_dup, "email"]) | existing

                rows = []
                for _, r in big.iterrows():
                    d = r.to_dict()
                    d["upload_date"] = "" # Default to empty string for all rows
                    is_dup = r["email"] in dup_set
                    d["duplicate"] = is_dup

                    if is_dup:
                        if r["email"] in existing_leads_map:
                            # Get the lead object from our map (no new DB query)
                            lead = existing_leads_map[r["email"]]
                            camp = lead.campaign or ""
                            qtr = lead.quarter or ""
                            d["origin"] = f"DB: {camp}/{qtr}" if (camp or qtr) else "DB"
                            # Add the formatted upload date to our dictionary
                            d["upload_date"] = lead.upload_date.strftime('%Y-%m-%d') if lead.upload_date else ''
                        else:
                            d["origin"] = "Current Sheet"
                    else:
                        d["origin"] = "Current Sheet"
                    rows.append(d)


                cache_path = Path(current_app.config['UPLOAD_FOLDER']) / f"_cache_{token}.parquet"
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
                # ---- DUPLICATE CHECKER PAGINATION (POST dedupe) ----
                dpage = request.args.get('dpage', 1, type=int)
                dper_page = 100
                dtotal = len(dedupe_ctx["rows"])
                start = (dpage - 1) * dper_page
                end = start + dper_page
                page_items = dedupe_ctx["rows"][start:end]

                dedupe_pagination = SimplePagination(
                    page=dpage, per_page=dper_page, total=dtotal, items=page_items
                )

                dedupe_ctx["rows"] = page_items


            # ===== Save (all or duplicates) =====
            elif action in ("save_all", "save_dup"):
                token = request.form.get("token", "")
                cache_path = Path(current_app.config['UPLOAD_FOLDER']) / f"_cache_{token}.parquet"
                if not cache_path.exists():
                    flash("No data to save; re-run check first", "warning")
                    return redirect(url_for("tools", tab="duplicate"))

                df = pd.read_parquet(cache_path)
                df.columns = [c.lower() for c in df.columns]
                if "campaign name" in df.columns:
                    df.rename(columns={"campaign name": "campaign"}, inplace=True)
                if "exclusions" not in df.columns:
                    df["exclusions"] = ""
                    
                df = df.where(pd.notnull(df), "")
                existing = {
        e for (e,) in db.session.query(Lead.email)
        .filter(
            Lead.team_id == current_user.team_id,   # ← team scope
            Lead.email.in_(df["email"])
        ).all()
    }

                mask_dup = df.duplicated("email", keep=False)
                dup_set = set(df.loc[mask_dup, "email"]) | existing

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
                    exclusions=r.get("exclusions", ""),
                    team_id=current_user.team_id        # ★ ADD THIS
                )

                    db.session.add(lead)
                    saved += 1

                db.session.commit()
                flash(f"{saved} rows saved", "success")
                return redirect(url_for("tools", tab="duplicate"))

            # ===== Exact email search =====
            elif action == "search":
                q = (request.form.get("search_email", "") or "").strip().lower()
                search_results = Lead.query.filter_by(email=q, team_id=current_user.team_id).all()
                if not search_results:
                    flash("No matches found", "info")
                active_tab = "search"

            # ===== Merge files =====
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
                merged = pd.concat([df.reindex(columns=hdrs, fill_value="") for df in dfs],
                                   ignore_index=True)

                token = uuid.uuid4().hex
                out_fn = f"merged_{token}.xlsx"
                merged.to_excel(Path(current_app.config['UPLOAD_FOLDER']) / out_fn,
                                index=False, engine="openpyxl")

                merge_ctx = {
                    "headers": hdrs,
                    "records": merged.to_dict("records"),
                    "download": out_fn
                }
                active_tab = "merge"

     # ---------- View Leads + filters (GET) with PAGINATION ----------
        viewdb_pagination = None
        if active_tab == 'viewdb':
            # Get the page number from URL, default to page 1
            page = request.args.get('page', 1, type=int)
            
            # Set how many leads to show per page
            per_page = 100

            query = Lead.query.filter(Lead.team_id == current_user.team_id)  # base team scope
            if request.args.get('enable_email') and request.args.get('filter_email'):
                query = query.filter(Lead.email.ilike(f"%{request.args.get('filter_email')}%"))
            if request.args.get('enable_campaign') and request.args.get('filter_campaign'):
                query = query.filter(Lead.campaign.ilike(f"%{request.args.get('filter_campaign')}%"))
            if request.args.get('enable_company') and request.args.get('filter_company'):
                query = query.filter(Lead.company.ilike(f"%{request.args.get('filter_company')}%"))
            if request.args.get('enable_source') and request.args.get('filter_source'):
                query = query.filter(Lead.source_file.ilike(f"%{request.args.get('filter_source')}%"))


            # This is the key change: .paginate() instead of .all()
            viewdb_pagination = query.order_by(Lead.upload_date.desc()).paginate(
                page=page, per_page=per_page, error_out=False
            )

        return render_template(
            "tools.html",
            active_tab=active_tab,
            dedupe=dedupe_ctx,
            search_results=search_results,
            merge=merge_ctx,
            # Pass the whole pagination object to the template
            pagination=viewdb_pagination, 
            dedupe_pagination=dedupe_pagination, 
        )

    @app.route("/uploads/<path:filename>")
    @login_required
    def uploaded_file(filename):
        return send_from_directory(app.config['UPLOAD_FOLDER'], filename)

    # --- BOOTSTRAP: create tables, then apply minimal schema + seed (idempotent)
    with app.app_context():
        # Create any missing tables (never drops anything)
        db.create_all()
        # Add missing columns/indexes, backfill team_id, add FKs, seed default teams/users
        ensure_min_schema_and_seed()

    return app

if __name__ == "__main__":
    application = create_app()
    application.run(
        host="0.0.0.0",
        port=int(os.getenv("PORT", 5000)),
        debug=True
    )

