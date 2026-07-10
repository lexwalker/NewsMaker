"""E2E test of the insertRows append on a SCRATCH tab (not the real one).
Scenario: batch1 -> editor writes D-verdict AND an H-column note ->
batch2 -> verify both stayed glued to their news row; separators intact;
dedup works. Scratch tab deleted at the end."""
import sys, warnings, os
from pathlib import Path
warnings.filterwarnings("ignore")
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT/"src")); sys.path.insert(0, str(ROOT/"scripts"))
from dotenv import load_dotenv; load_dotenv(ROOT/".env", override=True)
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
import review_tab

review_tab.REVIEW_TAB = "-- ТЕСТ review_tab (удалить) --"   # scratch
SA = ROOT / os.environ['GOOGLE_SERVICE_ACCOUNT_JSON'].lstrip('./')
svc = build('sheets','v4',credentials=Credentials.from_service_account_file(
    str(SA), scopes=['https://www.googleapis.com/auth/spreadsheets']), cache_discovery=False)
SID = os.environ['SPREADSHEET_ID']
T = review_tab.REVIEW_TAB

def rows_now():
    return svc.spreadsheets().values().get(
        spreadsheetId=SID, range=f"'{T}'!A1:H30").execute().get('values', [])

fails = []
try:
    # batch 1
    n1 = review_tab.append_batch(svc, SID, [
        {"title":"Новость Альфа","context":"дубль по сигнатуре","type":"дубль","url_hash":"hashA","url":"https://a.example/1"},
        {"title":"Новость Бета","context":"не новость","type":"не новость","url_hash":"hashB","url":"https://b.example/2"},
    ], "Прогон 1")
    print("batch1 added:", n1)
    # editor: D-verdict on Альфа + free note in H on Бета
    grid = rows_now()
    ia = next(i for i,r in enumerate(grid) if r and r[0]=="Новость Альфа")
    ib = next(i for i,r in enumerate(grid) if r and r[0]=="Новость Бета")
    svc.spreadsheets().values().update(spreadsheetId=SID, range=f"'{T}'!D{ia+1}",
        valueInputOption="RAW", body={"values":[["нет"]]}).execute()
    svc.spreadsheets().values().update(spreadsheetId=SID, range=f"'{T}'!H{ib+1}",
        valueInputOption="RAW", body={"values":[["комментарий редактора: было уже"]]}).execute()
    # batch 2 (+1 dup that must be skipped)
    n2 = review_tab.append_batch(svc, SID, [
        {"title":"Новость Гамма","context":"свежий сомнительный","type":"дубль","url_hash":"hashC","url":"https://c.example/3"},
        {"title":"Новость Альфа (дубль)","context":"same hash","type":"дубль","url_hash":"hashA","url":"https://a.example/1"},
    ], "Прогон 2")
    print("batch2 added:", n2, "(want 1 — hashA deduped)")
    grid = rows_now()
    def row_of(title):
        for i,r in enumerate(grid):
            if r and r[0]==title: return i,r
        return None,None
    ia,ra = row_of("Новость Альфа"); ib,rb = row_of("Новость Бета"); ic,rc = row_of("Новость Гамма")
    # checks
    if n2 != 1: fails.append(f"dedup: n2={n2}")
    if not ra or len(ra)<4 or ra[3]!="нет": fails.append(f"D-verdict lost: {ra}")
    if not rb or len(rb)<8 or rb[7]!="комментарий редактора: было уже": fails.append(f"H-note lost/misplaced: {rb}")
    if not rc: fails.append("Гамма missing")
    if ic is not None and ia is not None and not (ic < ia): fails.append("ordering: новый батч не сверху")
    seps=[r[0] for r in grid if r and r[0].startswith("━━")]
    if len(seps)!=2: fails.append(f"separators: {seps}")
    if grid[1][:3]!=review_tab.HEADER[:3]: fails.append(f"header row broken: {grid[1][:3]}")
    print("\nfinal grid (A,D,H):")
    for i,r in enumerate(grid):
        a=r[0] if r else ''; d=r[3] if r and len(r)>3 else ''; h=r[7] if r and len(r)>7 else ''
        print(f"  {i+1:2} | {a[:34]:34} | D={d:4} | H={h[:30]}")
    print("\nRESULT:", "ALL PASS" if not fails else f"FAILS: {fails}")
finally:
    # delete scratch tab
    meta=svc.spreadsheets().get(spreadsheetId=SID).execute()
    for s in meta['sheets']:
        if s['properties']['title']==T:
            svc.spreadsheets().batchUpdate(spreadsheetId=SID,
                body={"requests":[{"deleteSheet":{"sheetId":s['properties']['sheetId']}}]}).execute()
            print("scratch tab deleted")
