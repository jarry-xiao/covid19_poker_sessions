import argparse
import json
from heapq import heapify, heappush, heappop

from googleapiclient.discovery import build
import pandas as pd


def compute_transactions(ledger):
    assert round(sum(ledger.values()), 2) == 0
    neg = []
    pos = []
    for name, value in ledger.items():
        if value < 0:
            heappush(neg, (value, value, name))
        else:
            heappush(pos, (-value, value, name))
    transactions = []
    while neg and pos:
        _, debt, debtee = heappop(pos)
        _, payment, debtor = heappop(neg)
        unaccounted = round(debt + payment, 2)
        if unaccounted > 0:
            heappush(pos, (-unaccounted, unaccounted, debtee))
        elif unaccounted < 0:
            heappush(neg, (unaccounted, unaccounted, debtor))
        amount = min(debt, -payment)
        transactions.append((debtee, debtor, amount))
    assert len(neg) == 0
    assert len(pos) == 0
    transactions = sorted(transactions)
    return transactions


def get_spreadsheet_data(cfg):
    service = build('sheets', 'v4', developerKey=cfg["API_KEY"])
    sheet_api = service.spreadsheets()
    spreadsheet_metadata = (
        sheet_api.get(spreadsheetId=cfg["spreadsheet_id"])
        .execute()
    )
    weeks = {}
    venmo_info = {}
    for sheet in spreadsheet_metadata["sheets"]:
        name = sheet["properties"]["title"]
        result = (
            sheet_api.values()
            .get(spreadsheetId=cfg["spreadsheet_id"], range=name)
            .execute()["values"]
        )
        if name.startswith("Week"):
            columns = result[0] + ["PnL"]
            columns[0] = "Name"
            result = [row for row in result[3:] if len(row) == len(columns) and row[0]]
            df = pd.DataFrame(result, columns=columns)
            df.replace(r'^\s+', '0', regex=True, inplace=True)
            df.replace('', '0', regex=True, inplace=True)
            for column in columns[1:]:
                df[column] = df[column].astype(float)
            df = df[df.PnL != 0]
            df.PnL = df.PnL.round(decimals=2)
            assert abs(round(df.PnL.sum(), 2)) == 0
            weeks[int(name.replace("Week", ""))] = df
        else:
            for name, username in result[1:]:
                venmo_info[name] = username
    return weeks, venmo_info


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("config")
    parser.add_argument("--week", dest="week", type=int)
    args = parser.parse_args()
    with open(args.config) as data:
        cfg = json.load(data)
    weeks, venmo_info = get_spreadsheet_data(cfg)
    if not args.week or args.week not in weeks:
        index = max(weeks.keys())
    else:
        index = args.week
    ledger = dict(zip(weeks[index].Name, weeks[index].PnL))
    transactions = compute_transactions(ledger)
    for debtee, debtor, amount in transactions:
        venmo = venmo_info.get(debtor)
        if venmo:
            print(f"{debtee} requests ${amount} from {debtor} (@{venmo})")
        else:
            print(f"{debtee} requests ${amount} from {debtor}")
