import os
import sqlite3
from datetime import datetime
from email.message import EmailMessage
from typing import List, Tuple, Optional

import smtplib
from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas

DB_PATH = r".\data\holdiq.db"
OUT_DIR = r".\out"
PDF_DIR = os.path.join(OUT_DIR, "pdf")


# -------------------- SIMPLE PDF RENDER --------------------


def brief_md_to_html(brief_md: str, title: str) -> str:
    """
    Very basic conversion of markdown-ish text to HTML.
    Not a full markdown parser, just enough to look decent in email.
    """
    import html

    escaped = html.escape(brief_md)

    lines = escaped.splitlines()
    html_lines = []
    for line in lines:
        if not line.strip():
            html_lines.append("<br>")
        else:
            html_lines.append(line)

    body_html = "<br>\n".join(html_lines)

    return f"""
<html>
  <body style="font-family: Arial, sans-serif; font-size: 14px; line-height: 1.5;">
    <h2>{html.escape(title)}</h2>
    <div style="white-space: normal;">
      {body_html}
    </div>
  </body>
</html>
""".strip()


def brief_to_pdf(brief_md: str, pdf_path: str, title: str) -> None:
    """
    Very simple PDF renderer: writes plain text lines to pages.
    Enough for v1 emailed reports.
    """
    os.makedirs(os.path.dirname(pdf_path), exist_ok=True)

    c = canvas.Canvas(pdf_path, pagesize=letter)
    width, height = letter

    y = height - 50
    # Title
    c.setFont("Helvetica-Bold", 14)
    c.drawString(50, y, title)
    y -= 30

    c.setFont("Helvetica", 10)

    for line in brief_md.splitlines():
        if y < 50:
            c.showPage()
            y = height - 50
            c.setFont("Helvetica", 10)
        c.drawString(50, y, line[:110])
        y -= 12

    c.save()


# -------------------- EMAIL SENDER (SMTP) --------------------


def send_email_with_pdf(
    to_email: str,
    subject: str,
    brief_md: str,
    pdf_path: str,
    smtp_host: str,
    smtp_port: int,
    smtp_user: str,
    smtp_pass: str,
    from_email: str,
    title: str,
) -> None:
    msg = EmailMessage()
    msg["From"] = from_email
    msg["To"] = to_email
    msg["Subject"] = subject

    # Plain-text fallback
    msg.set_content(brief_md or "See attached PDF for full brief.")

    # HTML version
    html_body = brief_md_to_html(brief_md, title)
    msg.add_alternative(html_body, subtype="html")

    # Attach PDF
    with open(pdf_path, "rb") as f:
        pdf_data = f.read()
    msg.add_attachment(
        pdf_data,
        maintype="application",
        subtype="pdf",
        filename=os.path.basename(pdf_path),
    )

        # Connect using STARTTLS (Zoho: smtp.zoho.com:587)
    with smtplib.SMTP(smtp_host, smtp_port) as server:
        server.ehlo()
        server.starttls()
        server.ehlo()
        server.login(smtp_user, smtp_pass)
        server.send_message(msg)


# -------------------- SUBSCRIBER + BRIEF QUERY --------------------


def ensure_subscribers_table(con: sqlite3.Connection) -> None:
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS subscribers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            email TEXT NOT NULL,
            cik   TEXT NOT NULL,
            tier  TEXT NOT NULL CHECK (tier IN ('nano','mini','premium')),
            active INTEGER NOT NULL DEFAULT 1,
            billing_provider TEXT,
            customer_id TEXT,
            subscription_id TEXT,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
        """
    )
    con.commit()


def get_subscribers(con: sqlite3.Connection) -> List[Tuple[str, str, str]]:
    """
    Return (email, cik, tier) for all ACTIVE subscribers.
    """
    ensure_subscribers_table(con)
    return con.execute(
        """
        SELECT email, cik, tier
        FROM subscribers
        WHERE active = 1
        """
    ).fetchall()


def get_ai_brief_for(
    con: sqlite3.Connection, cik: str, tier: str
) -> Tuple[str, str, str]:
    """
    Map tier to model label, and fetch the latest brief for that manager.
    Returns (report_period, model_label, brief_md).
    """
    tier_to_model = {
        "nano": "gpt-5-nano",
        "mini": "gpt-5-mini",
        "premium": "gpt-5.1",
    }
    model_label = tier_to_model.get(tier, "gpt-5-nano")

    row = con.execute(
        """
        SELECT report_period, model, brief_md
        FROM ai_briefs
        WHERE manager_cik = ?
          AND model = ?
        ORDER BY created_at DESC
        LIMIT 1
        """,
        (cik, model_label),
    ).fetchone()

    if not row:
        raise RuntimeError(f"No brief found for CIK={cik}, model={model_label}")

    return row[0], row[1], row[2]


def get_manager_name(con: sqlite3.Connection, cik: str) -> Optional[str]:
    """
    Try to look up a human-readable manager name in managers table.
    If the table doesn't exist yet, just return None so we can fall back
    to 'Manager {cik}'.
    """
    try:
        row = con.execute(
            "SELECT managerName FROM managers WHERE managerCik = ? LIMIT 1",
            (cik,),
        ).fetchone()
    except sqlite3.OperationalError as e:
        # If managers table doesn't exist yet, just skip the lookup.
        if "no such table: managers" in str(e):
            return None
        # Any other sqlite error should still bubble up.
        raise

    if row:
        return row[0]
    return None


# -------------------- MAIN EMAIL DISPATCH --------------------


def send_all_brief_emails(con: sqlite3.Connection) -> None:
    """
    Core worker: for each active subscriber, fetch the appropriate brief,
    render a PDF, and email it.
    """
    subs = get_subscribers(con)
    if not subs:
        print("[email] No active subscribers found. Nothing to send.")
        return

    # SMTP config from environment (adjust names if you like)
    smtp_host = os.environ.get("HOLDIQ_SMTP_HOST")
    smtp_port = int(os.environ.get("HOLDIQ_SMTP_PORT", "465"))
    smtp_user = os.environ.get("HOLDIQ_SMTP_USER")
    smtp_pass = os.environ.get("HOLDIQ_SMTP_PASS")
    from_email = os.environ.get("HOLDIQ_FROM_EMAIL", smtp_user or "")

    if not (smtp_host and smtp_user and smtp_pass and from_email):
        print(
            "❌ SMTP config missing. Please set HOLDIQ_SMTP_HOST, "
            "HOLDIQ_SMTP_USER, HOLDIQ_SMTP_PASS, and HOLDIQ_FROM_EMAIL."
        )
        return

    print(f"[email] Found {len(subs)} active subscribers.")

    for email, cik, tier in subs:
        try:
            period, model_label, brief_md = get_ai_brief_for(con, cik, tier)
            mgr_name = get_manager_name(con, cik) or f"Manager {cik}"

            title = f"{mgr_name} — {period} — {model_label}"
            subject = f"HoldIQ {tier.capitalize()} Brief | {mgr_name} | {period}"

            safe_model = model_label.replace(".", "_")
            pdf_filename = f"brief_{cik}_{period}_{safe_model}.pdf"
            pdf_path = os.path.join(PDF_DIR, pdf_filename)

            print(
                f"[email] Sending {tier} brief to {email} "
                f"for CIK={cik}, period={period}, model={model_label}..."
            )

            brief_to_pdf(brief_md, pdf_path, title)

            send_email_with_pdf(
                to_email=email,
                subject=subject,
                brief_md=brief_md,
                pdf_path=pdf_path,
                smtp_host=smtp_host,
                smtp_port=smtp_port,
                smtp_user=smtp_user,
                smtp_pass=smtp_pass,
                from_email=from_email,
                title=title,
            )

            print(f"[email] ✅ Sent to {email} ({cik}, {period}, {model_label})")

        except Exception as e:
            import traceback

            print(
                f"[email] ❌ Error sending brief to {email} "
                f"(CIK={cik}, tier={tier}): {e}"
            )
            traceback.print_exc()


def main() -> None:
    """
    Entry point for send_brief_emails.py.

    This wraps the core email-sending function in a try/except so the script
    never crashes silently when run from Task Scheduler.
    """
    try:
        con = sqlite3.connect(DB_PATH)
        send_all_brief_emails(con)
        con.close()
    except Exception as e:
        import traceback

        print(f"❌ Error in send_brief_emails main(): {e}")
        traceback.print_exc()


if __name__ == "__main__":
    main()
