import os
from superset.app import create_app
from superset.extensions import db
from flask_appbuilder.security.sqla.models import User

app = create_app()
sm = app.appbuilder.sm

u  = os.environ.get("SUPERSET_ADMIN_USERNAME", "admin")
p  = os.environ.get("SUPERSET_ADMIN_PASSWORD", "admin")
fn = os.environ.get("SUPERSET_ADMIN_FIRSTNAME", "Admin")
ln = os.environ.get("SUPERSET_ADMIN_LASTNAME", "User")
em = os.environ.get("SUPERSET_ADMIN_EMAIL", "admin@local")

with app.app_context():
    user = db.session.query(User).filter(User.username == u).first()
    if user:
        print("[superset-init] admin exists")
    else:
        sm.add_user(u, fn, ln, em, sm.find_role("Admin"), password=p)
        db.session.commit()
        print("[superset-init] admin created")
