# BDIF11 Mailer

Minimal daily mailer for BTG Pactual BDIF11 documents.

It fetches BTG's public documents JSON endpoint for BDIF11 and emails only:

- Relatório Mensal
- Fatos Relevantes

The BDIF11 page itself is client-rendered, but the documents are available from:

```text
https://www.btgpactual.com/api/institutional/public/list/funds/40502607000194
```

No headless browser is required.

Sent documents are tracked in `sent_documents.json`.
Each PDF is sent in its own email. The subject and attachment filename use:

```text
YYYY-MM-DD BDIF11 [Type of Report]
YYYY-MM-DD BDIF11 [Type of Report].pdf
```

## Setup

```bash
python -m venv .venv
.\.venv\Scripts\activate
pip install -r requirements.txt
copy .env.example .env
```

Fill `.env` with:

```bash
RESEND_API_KEY=...
RESEND_FROM_EMAIL=...
RESEND_TO_EMAIL=...
```

`RESEND_TO_EMAIL` may contain one email or multiple comma-separated emails.

## Run

```bash
python main.py --dry-run
python main.py --dry-run --send-one
python main.py --send-one
python main.py
```

The first real run sends every historical matching document, one email per PDF.
Later runs send only documents missing from `sent_documents.json`.

Use `--send-one` to send only the newest unsent document. This is useful for
checking the email output and creating mailbox rules before processing the full
historical backlog. You can also use `--limit N` to process a specific number of
newest unsent documents.

If BTG returns a temporary error for one PDF, the script retries that download,
continues with the remaining documents, and keeps successful sends in
`sent_documents.json` so the failed PDF can be retried on the next run.

## GitHub Actions

Add these repository secrets or variables:

- `RESEND_API_KEY`
- `RESEND_FROM_EMAIL`
- `RESEND_TO_EMAIL`

The workflow runs weekdays at `0 14 * * 1-5`, which is 11:00 in Sao Paulo.
