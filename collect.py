#!/usr/bin/env python3
"""Collecteur nocturne de la veille Gabegie.

Récupère ce que l'outil de lecture de la veille ne peut pas lire :
  - titres des médias (flux RSS) + GDELT pour les médias qui bloquent les robots
  - Journal officiel (open data DILA)
  - marchés publics (BOAMP, TED)
  - rapports de la Cour des comptes / CRC en texte intégral

Sorties dans latest/ (écrasées à chaque run) et rapports/ (conservés 10 jours).
Chaque source échoue sans casser les autres. Voir latest/STATUS.md.
"""
import datetime as dt
import io
import json
import pathlib
import re
import subprocess
import tarfile
import time
import unicodedata

import feedparser
import requests
from bs4 import BeautifulSoup

ROOT = pathlib.Path(__file__).parent
LATEST = ROOT / "latest"
REPORTS = ROOT / "rapports"
STATE = ROOT / "state"
UA = "Mozilla/5.0 (compatible; GabegieVeille/1.0; +https://github.com/Sdimmix/veille)"
HEADERS = {"User-Agent": UA, "Accept-Language": "fr-FR,fr;q=0.9"}
NOW = dt.datetime.now(dt.timezone.utc)
TODAY = NOW.date()
status = []  # (source, ok, detail)


def log(source, ok, detail=""):
    status.append((source, ok, detail))
    print(("OK  " if ok else "FAIL"), source, detail, flush=True)


def get(url, **kw):
    kw.setdefault("timeout", 40)
    kw.setdefault("headers", HEADERS)
    r = requests.get(url, **kw)
    r.raise_for_status()
    return r


def norm(s):
    s = unicodedata.normalize("NFKD", s or "")
    s = "".join(c for c in s if not unicodedata.combining(c))
    return s.lower()


KEYWORDS = """argent public|depenses publiques|deniers publics|gaspillage|gabegie|dette|deficit|budget|plf|plfss|loi de finances|cour des comptes|chambre regionale des comptes|audit|rapport senatorial|rapport parlementaire|commission d'enquete|mission d'information|igf|igas|marche public|sans mise en concurrence|avenant|surcout|depassement|delegation de service public|concession|cabinet de conseil|cabinets de conseil|prestataire|appel d'offres|partenariat public|subvention|aide publique|aide sociale|fonds public|credit d'impot|niche fiscale|exoneration|pass culture|plan de relance|france 2030|secu|securite sociale|hopital|ame |aide medicale|fraude|arret maladie|assurance maladie|retraite|allocation|rsa|chomage|unedic|france travail|detournement|favoritisme|prise illegale|gestion de fait|abus de biens|parquet national financier|pnf|tracfin|pantouflage|nomination|nomme|remuneration|indemnite|salaire|cabinet ministeriel|frais de representation|logement de fonction|hatvp|region|departement|mairie|metropole|sncf|ratp|edf|la poste|sanef|autoroute|audiovisuel public|france televisions|operateur|agence|immigration|logement social|loyer|defense|ukraine|aide au developpement|milliard|million|cout|depense|credits|annulation de credits|decret d'avance|dotation|taxe|impot|cotisation|cnaf|cnav|urssaf|acoss""".split("|")
KW = [norm(k) for k in KEYWORDS]


def hits(text):
    t = norm(text)
    return sorted({k.strip() for k in KW if k in t})


# ---------------------------------------------------------------- PRESSE (RSS)
FEEDS = {
    "Le Monde - Une": "https://www.lemonde.fr/rss/une.xml",
    "Le Monde - Économie": "https://www.lemonde.fr/economie/rss_full.xml",
    "Le Monde - Politique": "https://www.lemonde.fr/politique/rss_full.xml",
    "Le Monde - Société": "https://www.lemonde.fr/societe/rss_full.xml",
    "Le Figaro - Actualités": "https://www.lefigaro.fr/rss/figaro_actualites.xml",
    "Le Figaro - Économie": "https://www.lefigaro.fr/rss/figaro_economie.xml",
    "Le Figaro - Politique": "https://www.lefigaro.fr/rss/figaro_politique.xml",
    "Le Figaro - Conjoncture": "https://www.lefigaro.fr/rss/figaro_conjoncture.xml",
    "Les Échos - Une": ["https://services.lesechos.fr/rss/les-echos-une.xml", "https://www.lesechos.fr/rss/rss_une.xml", "https://www.lesechos.fr/rss.xml"],
    "Les Échos - Économie": ["https://services.lesechos.fr/rss/les-echos-economie.xml", "https://www.lesechos.fr/rss/rss_economie.xml"],
    "Les Échos - Politique": ["https://services.lesechos.fr/rss/les-echos-politique.xml", "https://www.lesechos.fr/rss/rss_politique.xml"],
    "Libération": "https://www.liberation.fr/arc/outboundfeeds/rss-all/?outputType=xml",
    "Franceinfo - Titres": "https://www.francetvinfo.fr/titres.rss",
    "Franceinfo - Économie": "https://www.francetvinfo.fr/economie.rss",
    "Franceinfo - Politique": "https://www.francetvinfo.fr/politique.rss",
    "Le Parisien - Une": "https://feeds.leparisien.fr/leparisien/rss",
    "Le Parisien - Économie": "https://feeds.leparisien.fr/leparisien/rss/economie",
    "Le Parisien - Politique": "https://feeds.leparisien.fr/leparisien/rss/politique",
    "La Croix": "https://www.la-croix.com/RSS",
    "L'Humanité": "https://www.humanite.fr/rss/actu.rss",
    "L'Opinion": "https://www.lopinion.fr/index.rss",
    "Marianne": ["https://www.marianne.net/rss.xml", "https://www.marianne.net/rss", "https://www.marianne.net/feed"],
    "Le Point": ["https://www.lepoint.fr/rss.xml", "https://www.lepoint.fr/rss/", "https://www.lepoint.fr/politique/rss.xml", "https://www.lepoint.fr/economie/rss.xml"],
    "L'Express": "https://www.lexpress.fr/rss/alaune.xml",
    "Challenges": "https://www.challenges.fr/rss.xml",
    "La Tribune": "https://www.latribune.fr/rss/rubriques/actualite.html",
    "Mediapart": "https://www.mediapart.fr/articles/feed",
    "BFM - Économie": "https://www.bfmtv.com/rss/economie/",
    "BFM - Politique": "https://www.bfmtv.com/rss/politique/",
    "RMC": "https://rmc.bfmtv.com/rss/actualites/",
    "Europe 1": "https://www.europe1.fr/rss.xml",
    "Public Sénat": "https://www.publicsenat.fr/rss",
    "LCP": "https://lcp.fr/rss.xml",
    "Acteurs publics": ["https://acteurspublics.fr/rss", "https://acteurspublics.fr/rss.xml", "https://acteurspublics.fr/feed", "https://acteurspublics.fr/rss/actualites"],
    "Gazette des communes": ["https://www.lagazettedescommunes.com/feed/", "https://www.lagazettedescommunes.com/rss"],
    "Banque des Territoires": ["https://www.banquedesterritoires.fr/rss.xml", "https://www.banquedesterritoires.fr/rss", "https://www.banquedesterritoires.fr/actualites/rss.xml"],
    "Capital": ["https://www.capital.fr/rss", "https://www.capital.fr/rss.xml", "https://www.capital.fr/feed", "https://www.capital.fr/economie-politique/rss"],
    "Ouest-France": "https://www.ouest-france.fr/rss/une",
    "20 Minutes": "https://www.20minutes.fr/feeds/rss-une.xml",
    "Le JDD": "https://www.lejdd.fr/rss.xml",
    "Reporterre": "https://reporterre.net/spip.php?page=backend",
}


def entry_date(e):
    for k in ("published_parsed", "updated_parsed"):
        v = e.get(k)
        if v:
            return dt.datetime(*v[:6], tzinfo=dt.timezone.utc)
    return None


def collect_rss():
    rows = []
    for name, url in FEEDS.items():
        try:
            urls = url if isinstance(url, list) else [url]
            feed, last = None, None
            for u in urls:
                try:
                    r = get(u, timeout=25)
                    f = feedparser.parse(r.content)
                    if f.entries:
                        feed = f
                        break
                    last = f"{u} : flux vide"
                except Exception as ex2:
                    last = f"{u} : {str(ex2)[:80]}"
            if feed is None:
                raise RuntimeError(last or "aucun flux")
            n = 0
            for e in feed.entries:
                d = entry_date(e)
                if d and NOW - d > dt.timedelta(hours=72):
                    continue
                title = (e.get("title") or "").strip()
                summ = BeautifulSoup(e.get("summary", "") or "", "lxml").get_text(" ", strip=True)[:350]
                rows.append({"media": name, "title": title, "date": d.isoformat()[:16] if d else "?",
                             "link": e.get("link", ""), "summary": summ, "kw": hits(title + " " + summ)})
                n += 1
            log(f"RSS {name}", n > 0, f"{n} articles <72h ({len(feed.entries)} dans le flux)")
        except Exception as ex:
            log(f"RSS {name}", False, str(ex)[:120])
    return rows


GDELT_QUERIES = [
    '"argent public"', '"Cour des comptes"', '"dépenses publiques"', "gaspillage", '"marché public"',
    "subvention", '"déficit" budget', '"fraude sociale"', "pantouflage", '"commission d\'enquête"',
    '"chambre régionale des comptes"', '"détournement de fonds"', '"PLFSS"', '"aide médicale"',
    '"avenant" marché', '"cabinets de conseil"', "Sanef autoroutes",
]


def collect_gdelt():
    rows = []
    ok = 0
    for q in GDELT_QUERIES:
        for attempt in (1, 2):
            try:
                r = requests.get("https://api.gdeltproject.org/api/v2/doc/doc",
                                 params={"query": q + " sourcelang:french", "mode": "artlist",
                                         "maxrecords": 60, "format": "json", "timespan": "48h", "sort": "datedesc"},
                                 headers=HEADERS, timeout=40)
                try:
                    data = r.json()
                except Exception:
                    raise RuntimeError(f"HTTP {r.status_code} non JSON : {r.text[:120]!r}")
                arts = data.get("articles", [])
                for a in arts:
                    rows.append({"q": q, "title": a.get("title", ""), "domain": a.get("domain", ""),
                                 "date": a.get("seendate", ""), "link": a.get("url", "")})
                ok += 1
                break
            except Exception as ex:
                if attempt == 2:
                    log(f"GDELT {q}", False, str(ex)[:200])
                else:
                    time.sleep(12)
        time.sleep(8)  # limite GDELT : 1 requête / 5 s
    log("GDELT", ok > 0, f"{ok}/{len(GDELT_QUERIES)} requêtes OK, {len(rows)} articles")
    return rows


def write_presse(rss, gd):
    LATEST.mkdir(exist_ok=True)
    out = ["# Presse - titres des dernières 72 h (flux RSS)\n",
           "Légende : [mots-clés repérés]. Les articles payants ne sont pas lus, seulement titre et chapô.\n"]
    flagged = [r for r in rss if r["kw"]]
    out.append(f"\n## A. Titres avec mots-clés argent public ({len(flagged)})\n")
    for r in sorted(flagged, key=lambda x: (-len(x["kw"]), x["date"]), reverse=False)[:700]:
        out.append(f"- **{r['media']}** | {r['date']} | {r['title']} | [{', '.join(r['kw'][:6])}] | {r['link']}")
        if r["summary"]:
            out.append(f"    - {r['summary']}")
    out.append(f"\n## B. Tous les autres titres ({len(rss) - len(flagged)})\n")
    for r in rss:
        if not r["kw"]:
            out.append(f"- {r['media']} | {r['date']} | {r['title']} | {r['link']}")
    (LATEST / "presse.md").write_text("\n".join(out), encoding="utf-8")

    g = ["# GDELT - articles français des dernières 48 h par thème (médias bloquant les robots inclus)\n"]
    by_q = {}
    for a in gd:
        by_q.setdefault(a["q"], []).append(a)
    for q, arts in by_q.items():
        g.append(f"\n## {q} ({len(arts)})\n")
        for a in arts:
            g.append(f"- {a['domain']} | {a['date']} | {a['title']} | {a['link']}")
    pass


# ---------------------------------------------------------------- JOURNAL OFFICIEL (DILA)
JO_FLAG = ["nomination", "est nomme", "sont nommes", "nommee", "credit", "annulation", "decret d'avance", "subvention",
           "remuneration", "indemnite", "marche public", "budget", "dotation", "taxe", "plafond", "tarif", "prime",
           "cour des comptes", "contribution", "etablissement public", "societe", "fonds", "commission nationale",
           "inspection", "autorite", "conseil d'administration", "president du conseil", "directeur general",
           "securite sociale", "financement", "emprunt", "garantie de l'etat", "cession", "concession"]


def collect_jo():
    base = "https://echanges.dila.gouv.fr/OPENDATA/JORF/"
    r = get(base)
    names = sorted(set(re.findall(r'(JORF_(\d{8})-(\d{6})\.tar\.gz)', r.text)))
    cutoff = (TODAY - dt.timedelta(days=3)).strftime("%Y%m%d")
    recent = [n for n in names if n[1] >= cutoff]
    items = []
    members_seen, sample = 0, []
    for fname, d, _ in recent[-4:]:
        try:
            data = get(base + fname, timeout=180).content
            tf = tarfile.open(fileobj=io.BytesIO(data), mode="r:gz")
            for m in tf:
                members_seen += 1
                if len(sample) < 6 and m.isfile():
                    sample.append(m.name)
                bn = m.name.rsplit("/", 1)[-1]
                if not (m.isfile() and re.match(r"JORFTEXT\d+\.xml$", bn)) or "/struct/" in m.name:
                    continue
                xml = tf.extractfile(m).read().decode("utf-8", "ignore")
                s = BeautifulSoup(xml, "xml")

                def t(tag):
                    x = s.find(tag)
                    return x.get_text(" ", strip=True) if x else ""

                titre = t("TITREFULL") or t("TITRE")
                items.append({"id": t("ID") or bn[:-4], "nature": t("NATURE"), "titre": titre, "nor": t("NOR"),
                              "min": t("MINISTERE"), "date": t("DATE_PUBLI") or t("DATE_TEXTE"), "src": fname})
        except Exception as ex:
            log(f"JO {fname}", False, str(ex)[:120])
    log("JO diagnostic", True, f"{members_seen} fichiers dans les archives, exemples : {sample}")
    seen, uniq = set(), []
    for it in items:
        if it["id"] in seen:
            continue
        seen.add(it["id"])
        uniq.append(it)
    flagged = [i for i in uniq if any(k in norm(i["titre"] + " " + i["min"]) for k in JO_FLAG)]
    out = [f"# Journal officiel - textes parus (open data DILA, archives {', '.join(n[0] for n in recent[-4:])})\n",
           f"{len(uniq)} textes, dont {len(flagged)} avec mots-clés (nominations, crédits, subventions, etc.)\n",
           "\n## A. Textes à regarder\n"]
    for i in sorted(flagged, key=lambda x: x["date"], reverse=True):
        out.append(f"- {i['date']} | {i['nature']} | {i['titre']} | {i['min']} | https://www.legifrance.gouv.fr/jorf/id/{i['id']}")
    out.append("\n## B. Autres textes\n")
    flagged_ids = {i["id"] for i in flagged}
    for i in sorted(uniq, key=lambda x: x["date"], reverse=True):
        if i["id"] not in flagged_ids:
            out.append(f"- {i['date']} | {i['nature']} | {i['titre'][:220]} | https://www.legifrance.gouv.fr/jorf/id/{i['id']}")
    (LATEST / "jo.md").write_text("\n".join(out), encoding="utf-8")
    log("JO (DILA)", len(uniq) > 0, f"{len(uniq)} textes, {len(flagged)} signalés, archives: {[n[0] for n in recent[-4:]]}")


# ---------------------------------------------------------------- MARCHÉS PUBLICS
BOAMP_FLAG = ["avenant", "sans mise en concurrence", "sans publicite", "negocie sans", "cabinet de conseil",
              "prestations de conseil", "conseil en", "assistance a maitrise", "audit", "prestations intellectuelles",
              "communication", "evenementiel", "etude strategique", "conseil strategique"]


def collect_boamp():
    since = (TODAY - dt.timedelta(days=3)).isoformat()
    url = "https://boamp-datadila.opendatasoft.com/api/explore/v2.1/catalog/datasets/boamp/records"
    recs, keys = [], set()
    for off in range(0, 500, 100):
        r = get(url, params={"limit": 100, "offset": off, "order_by": "dateparution desc",
                             "where": f"dateparution>=date'{since}'"})
        res = r.json().get("results", [])
        if not res:
            break
        recs += res
    for x in recs[:1]:
        keys = set(x.keys())
    out = ["# BOAMP - avis publiés depuis 3 jours\n", f"{len(recs)} avis. Champs disponibles : {sorted(keys)}\n",
           "\n## A. Avis signalés (avenants, prestations intellectuelles, conseil, communication...)\n"]
    keep = [k for k in ("dateparution", "nomacheteur", "objet", "nature_categorise_libelle", "type_marche",
                        "famille_libelle", "montant", "valeur", "titulaire", "code_departement", "url_avis", "idweb")
            if k in keys]
    flagged = 0
    for x in recs:
        txt = norm(json.dumps(x, ensure_ascii=False)[:3000])
        if any(k in txt for k in BOAMP_FLAG):
            flagged += 1
            out.append("- " + json.dumps({k: x.get(k) for k in keep}, ensure_ascii=False))
    out.append("\n## B. Tous les avis (objet seulement)\n")
    for x in recs:
        out.append(f"- {x.get('dateparution')} | {x.get('nomacheteur')} | {str(x.get('objet'))[:200]}")
    (LATEST / "marches_boamp.md").write_text("\n".join(out), encoding="utf-8")
    log("BOAMP", len(recs) > 0, f"{len(recs)} avis, {flagged} signalés")


def collect_ted():
    since = (TODAY - dt.timedelta(days=3)).strftime("%Y%m%d")
    body = {"query": f"buyer-country=FRA AND publication-date>={since}",
            "fields": ["publication-number", "notice-title", "buyer-name", "publication-date", "total-value"],
            "limit": 100, "page": 1, "scope": "ACTIVE", "paginationMode": "PAGE_NUMBER"}
    r = requests.post("https://api.ted.europa.eu/v3/notices/search", json=body, headers=HEADERS, timeout=40)
    r.raise_for_status()
    notices = r.json().get("notices", [])
    out = ["# TED - avis français depuis 3 jours\n", f"{len(notices)} avis\n"]
    for n in notices:
        num = n.get("publication-number", "")
        buyer = n.get("buyer-name")
        title = n.get("notice-title")
        out.append("- " + json.dumps({"n": num, "date": n.get("publication-date"), "acheteur": buyer,
                                      "titre": title, "valeur": n.get("total-value"),
                                      "url": f"https://ted.europa.eu/fr/notice/{num}/html"}, ensure_ascii=False)[:600])
    (LATEST / "marches_ted.md").write_text("\n".join(out), encoding="utf-8")
    log("TED", len(notices) > 0, f"{len(notices)} avis")


# ---------------------------------------------------------------- RAPPORTS COUR DES COMPTES
def slug(s):
    return re.sub(r"[^a-z0-9]+", "-", norm(s)).strip("-")[:80]


def collect_ccomptes():
    STATE.mkdir(exist_ok=True)
    REPORTS.mkdir(exist_ok=True)
    sp = STATE / "ccomptes.json"
    state = json.loads(sp.read_text()) if sp.exists() else {}
    base = "https://www.ccomptes.fr"
    links = []
    for page in (0, 1):
        try:
            soup = BeautifulSoup(get(f"{base}/fr/publications", params={"page": page}).text, "lxml")
        except Exception as ex:
            log(f"Cour des comptes liste p{page}", False, str(ex)[:100])
            continue
        for a in soup.find_all("a", href=True):
            h = a["href"]
            if re.match(r"^/fr/publications/[^/?#]+$", h) and h not in [l[0] for l in links]:
                links.append((h, a.get_text(" ", strip=True)))
    new = 0
    for h, title in links:
        if h in state or new >= 12:
            continue
        try:
            soup = BeautifulSoup(get(base + h).text, "lxml")
            title = (soup.find("h1").get_text(" ", strip=True) if soup.find("h1") else title) or h
            pdfs = [a["href"] for a in soup.find_all("a", href=True) if a["href"].lower().split("?")[0].endswith(".pdf")]
            if not pdfs:
                state[h] = {"title": title, "first_seen": TODAY.isoformat(), "pdf": None}
                continue
            pdf = pdfs[0] if pdfs[0].startswith("http") else base + pdfs[0]
            data = get(pdf, timeout=120).content
            if len(data) > 60_000_000:
                raise RuntimeError("PDF trop gros")
            tmp = ROOT / "tmp.pdf"
            tmp.write_bytes(data)
            txt = subprocess.run(["pdftotext", "-layout", str(tmp), "-"], capture_output=True, text=True).stdout
            fn = f"{TODAY.isoformat()}_ccomptes_{slug(title)}.txt"
            (REPORTS / fn).write_text(f"TITRE: {title}\nPAGE: {base}{h}\nPDF: {pdf}\n\n{txt}", encoding="utf-8")
            state[h] = {"title": title, "first_seen": TODAY.isoformat(), "pdf": pdf, "file": fn, "chars": len(txt)}
            new += 1
        except Exception as ex:
            log(f"Cour des comptes {h}", False, str(ex)[:100])
    # purge > 10 jours
    for h, v in list(state.items()):
        if v.get("file") and (TODAY - dt.date.fromisoformat(v["first_seen"])).days > 10:
            (REPORTS / v["file"]).unlink(missing_ok=True)
            v["file"] = None
    sp.write_text(json.dumps(state, ensure_ascii=False, indent=1))
    idx = ["# Rapports Cour des comptes / CRC (texte intégral dans rapports/)\n"]
    for h, v in sorted(state.items(), key=lambda kv: kv[1]["first_seen"], reverse=True):
        if v.get("file"):
            idx.append(f"- {v['first_seen']} | {v['title']} | {v['chars']} caractères | rapports/{v['file']} | {base}{h}")
    (LATEST / "rapports_index.md").write_text("\n".join(idx), encoding="utf-8")
    log("Cour des comptes", len(links) > 0, f"{len(links)} publications listées, {new} nouveaux PDF lus")


def main():
    LATEST.mkdir(exist_ok=True)
    rss, gd = [], []
    for name, fn in [("rss", lambda: rss.extend(collect_rss())), ("jo", collect_jo), ("boamp", collect_boamp), ("ted", collect_ted),
                     ("ccomptes", collect_ccomptes)]:
        try:
            fn()
        except Exception as ex:
            log(name, False, f"{type(ex).__name__}: {str(ex)[:150]}")
    write_presse(rss, gd)
    st = [f"# STATUS collecte - {NOW.isoformat()[:16]} UTC\n"]
    for s, ok, d in status:
        st.append(f"- {'OK  ' if ok else 'ÉCHEC'} {s} {d}")
    (LATEST / "STATUS.md").write_text("\n".join(st), encoding="utf-8")


if __name__ == "__main__":
    main()
