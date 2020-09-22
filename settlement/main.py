from enum import Enum
from pathlib import Path

import pandas as pd
import typer
from typer import Option


class Column(str, Enum):
    name = "項目"
    price = "金額"
    debtors = "借りた人"
    creditor = "貸した人"
    not_include_creditor = "本人含まない"
    date = "日付"
    # created columns
    transaction_id = "貸し借りID"
    num_debtors = "借りた人の人数"
    price_per_person = "1人あたりの金額"
    debtor = "借りた人（個人）"
    debt = "借りた金額"
    credit = "貸した金額"


col = Column


def main():
    typer.run(calc_debt_credit)


def calc_debt_credit(
    csv_path: Path,
    intermediate_csv_path: Path = Option(
        None,
        "--intermediate-csv-path",
        "-o",
        help="Path where intermediate csv file will be saved.",
    ),
):
    df = (
        pd.read_csv(csv_path)
        .reset_index()
        .rename(columns={"index": col.transaction_id})
    )

    df[col.num_debtors] = df[col.debtors].apply(lambda x: len(x.split(",")))
    # if the creditor is included, add 1 to the number of debtors
    df[col.num_debtors] += df[col.not_include_creditor].apply(
        lambda x: 0 if x == "Yes" else 1
    )

    df[col.price_per_person] = df[col.price] / df[col.num_debtors]

    # split rows by debtors
    df = df.merge(
        pd.concat(
            [
                pd.Series(row[col.transaction_id], row[col.debtors].split(", "))
                for _, row in df.iterrows()
            ]
        )
        .reset_index()
        .rename(columns={"index": col.debtor, 0: col.transaction_id}),
        on=col.transaction_id,
    )
    if intermediate_csv_path:
        df[
            [
                col.date,
                col.name,
                col.creditor,
                col.debtor,
                col.price_per_person,
                col.price,
                col.num_debtors,
                col.debtors,
                col.not_include_creditor,
            ]
        ].to_csv(intermediate_csv_path)

    df = pd.concat(
        [
            df.groupby(col.creditor)[col.price_per_person].sum().rename(col.credit),
            df.groupby(col.debtor)[col.price_per_person].sum().rename(col.debt),
        ],
        axis=1,
        sort=True,
    ).fillna(0)
    df = df[col.debt] - df[col.credit]

    print(df.rename("収支"))
    print()
    print("負の値→貸している。その分、誰かから返してもらう。")
    print("正の値→借りている。その分、誰かに返す。")
