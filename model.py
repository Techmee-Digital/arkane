from datetime import datetime

from flask_sqlalchemy import SQLAlchemy
from flask_login import UserMixin
from werkzeug.security import generate_password_hash, check_password_hash

db = SQLAlchemy()


class Team(db.Model):
    __tablename__ = "teams"

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    name = db.Column(db.String(64), unique=True, nullable=False)

    # Relationships (optional, but handy if you want backrefs)
    users = db.relationship("User", backref="team", lazy=True)
    leads = db.relationship("Lead", backref="team", lazy=True)


class Lead(db.Model):
    __tablename__ = "leads"

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    email = db.Column(db.String(255), nullable=False, index=True)
    company = db.Column(db.String(255), default="")
    quarter = db.Column(db.String(10), default="")
    campaign = db.Column(db.String(100), default="")
    source_file = db.Column(db.String(255), default="")
    exclusions = db.Column(db.String(255), default="")
    reason = db.Column(db.String(500), default="")  # ← NEW LINE
    upload_date = db.Column(db.DateTime, default=datetime.utcnow, nullable=False, index=True)

    # ★ Multi-tenancy: every lead belongs to a team
    team_id = db.Column(db.Integer, db.ForeignKey("teams.id"), nullable=False, index=True)

    __table_args__ = (
        # Useful for fast per-team duplicate checks/lookups by email
        db.Index("ix_leads_team_email", "team_id", "email"),
    )
class Rejection(db.Model):
    __tablename__ = "rejections"

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    email = db.Column(db.String(255), nullable=False, index=True)
    company = db.Column(db.String(255), default="")
    campaign = db.Column(db.String(255), default="")
    reason = db.Column(db.String(500), default="")
    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)

    # Link to team (multi-tenancy)
    team_id = db.Column(db.Integer, db.ForeignKey("teams.id"), nullable=False, index=True)

    __table_args__ = (
        db.Index("ix_rejections_team_email", "team_id", "email"),
    )


class User(UserMixin, db.Model):
    __tablename__ = "users"

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    username = db.Column(db.String(64), unique=True, nullable=False, index=True)
    password_hash = db.Column(db.String(255), nullable=False)

    # ★ Multi-tenancy: every user belongs to a team
    team_id = db.Column(db.Integer, db.ForeignKey("teams.id"), nullable=False, index=True)

    # Optional role if you later want a superadmin/admin/member split
    role = db.Column(db.String(32), default="member")  # "member" | "admin" | "superadmin"

    def set_password(self, password: str):
        self.password_hash = generate_password_hash(password)

    def check_password(self, password: str) -> bool:
        return check_password_hash(self.password_hash, password)

    @property
    def is_superadmin(self) -> bool:
        return (self.role or "").lower() == "superadmin"
