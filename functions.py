import os
import pandas as pd
import numpy as np
import json
import requests
from bs4 import BeautifulSoup
import re


# base_path = r"J:\Mi unidad\1 - Data Science\Proyectos personales\mtg_decklists_analyzer\Decklists"
base_path = r"Decklists"


def download_decklists(deck_name, format, n_pages):
    
    ### Download ids of decklists for a given deck archetype and the number of pages
    print(f"Downloading ids of decklists for deck {deck_name}...")
    deck_name = deck_name.replace(" ", "%20")
    base_url = f"https://www.tcdecks.net/archetype.php?format=Premodern&archetype={deck_name}&page="
    decks_ids = []
    for page in range(1, n_pages+1):
        print(f"\r\tFetching page {page}/{n_pages}...", end="")
        url = f"{base_url}{page}"
        response = requests.get(url)
        soup = BeautifulSoup(response.text, 'html.parser')
        pattern = re.compile(r'deck\.php\?id=(\d+)&iddeck=(\d+)')
        page_decks_ids = [(int(id_), int(iddeck)) for id_, iddeck in (pattern.match(a['href']).groups() for a in soup.find_all('a', href=True) if pattern.match(a['href']))]
        page_decks_ids = list(set(page_decks_ids))
        decks_ids.extend(page_decks_ids)
    print("\r\tDone!\n")
    
    ### Download decklists for each deck id
    print(f"Downloading decklists...")
    decklists = []
    for i, (id_, iddeck) in enumerate(decks_ids):
        print(f"\r\tFetching {i+1}/{len(decks_ids)}: id {id_}, iddeck {iddeck}...", end="")
        url = f"https://www.tcdecks.net/download.php?ext=txt&id={id_}&iddeck={iddeck}"
        r = requests.get(url)
        decklists.append(r.text)
    print("\r\tDone!\n")

    ### Save decklists to txt files
    full_path = os.path.join(base_path, format, deck_name.replace("%20", " "))
    os.makedirs(full_path, exist_ok=True)
    print(f"Saving decklists to '{full_path}'...")
    digits_len = len(str(len(decks_ids)))
    for i, decklist in enumerate(decklists):
        i_str = str(i).zfill(digits_len)
        print(f"\r\tSaving decklist {i+1}/{len(decks_ids)}...", end="")
        final_path = os.path.join(full_path, f'decklist_{i_str}.txt')
        with open(final_path, 'w') as f:
            f.write(decklist.replace('\n', ''))
    print("\r\tDone!\n")


def read_decklists(deck_name, format):
    format = format.lower()
    path = os.path.join(base_path, format, deck_name)
    df = pd.DataFrame()
    for file in os.listdir(path):
        # Check whether file is in text format or not
        if file.endswith(".txt"):
            file_path = os.path.join(path, file)
            df = pd.concat([df, pd.read_csv(file_path, header=None, skip_blank_lines=False, sep="*")], axis=1)
    df.columns = list(range(len(df.columns)))
    return df


def process_decklists(df, format):
    
    def _read_n_update_cards_db(name_values:pd.Series, cards_db_json_path="cards_database.json"):
        # Load cards_database dict
        try:
            with open(cards_db_json_path, "r") as file:
                cards_database = json.load(file)
        except:
            cards_database = {format: {}}
        # Get card names
        unique_name_values = name_values.unique()
        # Add missing cards to cards_database
        for name in unique_name_values:
            if name not in cards_database[format]:
                cards_database[format][name] = {"type": "unknown", "subtype": "unknown", "color": "unknown"}
        # Save cards_database dict
        with open(cards_db_json_path, "w") as file:
            json.dump(cards_database, file)
        return cards_database
    
    
    def _read_cards_db(name_values:pd.Series, cards_db_excel_path="cards_database.xlsx", cards_db_json_path="cards_database.json"):
        cards_db_excel = pd.read_excel(cards_db_excel_path)
        cards_database = _read_n_update_cards_db(name_values, cards_db_json_path)
        for row in cards_db_excel.iterrows():
            row_list = row[1].values
            if row_list[0] not in cards_database:
                cards_database[row_list[0]] = {}
            if row_list[1] not in cards_database[row_list[0]]:
                cards_database[row_list[0]][row_list[1]] = {}
                cards_database[row_list[0]][row_list[1]]["type"] = row_list[2]
                cards_database[row_list[0]][row_list[1]]["subtype"] = row_list[3]
                cards_database[row_list[0]][row_list[1]]["color"] = row_list[4]
        # Save cards_database dict
        with open(cards_db_json_path, "w") as file:
            json.dump(cards_database, file)
        return cards_database


    # Change df to long format and add columns for sb, qty and name
    df = pd.melt(df, var_name= "#dl", value_name="cards")
    df["sb"] = 0
    df[["qty", "name"]] = df["cards"].str.strip().str.split(" ", n=1, expand=True)
    df = df.drop("cards", axis=1)

    # Separation of main and sb using NaNs
    for decklist_no in df["#dl"].unique():
        index_start = df.loc[df["#dl"]==decklist_no].iloc[0].name
        index_sb = df.loc[(df["#dl"]==decklist_no) & (df["qty"].isna()), "name"].index[0]
        index_end = index_start + df.loc[df["#dl"]==decklist_no].shape[0]
        df.iloc[index_sb:index_end, 1] = 1

    # Dropa NaNs after separation of main and sb
    df = df.dropna(ignore_index=True)

    # Change qty to integer
    df["qty"] = df["qty"].astype(int)
    

    #### Fill missing cards to have all the possible cards in every decklist

    # Separate main and sideboard
    df_main = df[df['sb'] == 0]
    df_sideboard = df[df['sb'] == 1]
    # Get unique values
    dl_values = df['#dl'].unique()
    name_values_main = df_main['name'].unique()
    name_values_sideboard = df_sideboard['name'].unique()
    # Create full index
    index_main = pd.MultiIndex.from_product([dl_values, name_values_main], names=['#dl', 'name'])
    index_sideboard = pd.MultiIndex.from_product([dl_values, name_values_sideboard], names=['#dl', 'name'])
    # Create full dataframes
    df_full_main = pd.DataFrame(index=index_main).reset_index()
    df_full_main['sb'] = 0
    df_full_sideboard = pd.DataFrame(index=index_sideboard).reset_index()
    df_full_sideboard['sb'] = 1
    # Merge with original dataframes
    df_main = pd.merge(df_full_main, df_main, on=['#dl', 'name', 'sb'], how='left')
    df_sideboard = pd.merge(df_full_sideboard, df_sideboard, on=['#dl', 'name', 'sb'], how='left')
    # Fill NaNs with 0
    df_main['qty'] = df_main['qty'].fillna(0).astype(int)
    df_sideboard['qty'] = df_sideboard['qty'].fillna(0).astype(int)
    # Concatenate main and sideboard
    df = pd.concat([df_main, df_sideboard])

    #### Include card type

    # Read, update and populate cards database
    cards_database = _read_cards_db(name_values=df["name"],
                                    cards_db_excel_path="cards_database.xlsx",
                                    cards_db_json_path="cards_database.json")
    # Include card type, subtype and color
    df["type"] = df["name"].map(cards_database[format]).str["type"]
    df["subtype"] = df["name"].map(cards_database[format]).str["subtype"]
    df["color"] = df["name"].map(cards_database[format]).str["color"]

    # Reorder columns
    df = df[["#dl", "sb", "type", "subtype", "color", "qty", "name"]]
    # Sort by decklist number and sb
    df = df.sort_values(by=["#dl", "sb", "type", "subtype", "qty", "color", "name"], ascending=[True, True, True, True, False, True, True])
    # Reset index
    df = df.reset_index(drop=True)

    return df


def analyze_dls_cards(df, types=False):

    def _calculate_final_qty_cards(x, n_decks):
        probabilities = [
            (x==((x.mean().round(0))-2)).sum() / n_decks,
            (x==((x.mean().round(0))-1)).sum() / n_decks,
            (x==((x.mean().round(0))+0)).sum() / n_decks,
            (x==((x.mean().round(0))+1)).sum() / n_decks,
            (x==((x.mean().round(0))+2)).sum() / n_decks,
        ]
        mean_diff = probabilities.index(max(probabilities)) - 2
        return x.mean().round(0) + mean_diff

    
    def _adjust_final_qty(df):
        for sb in range(2):
            adj_qty = df[df.sb==sb]["final_qty"].sum()
            target = 60 if sb == 0 else 15
            while adj_qty != target:
                if adj_qty > target:
                    df.loc[df[df.sb==sb]["diff"].idxmax(), "final_qty"] -= 1
                    df.loc[df[df.sb==sb]["diff"].idxmax(), "diff"] -= 1
                else:
                    df.loc[df[df.sb==sb]["diff"].idxmin(), "final_qty"] += 1
                    df.loc[df[df.sb==sb]["diff"].idxmin(), "diff"] += 1
                adj_qty = df[df.sb==sb]["final_qty"].sum()
        return df


    if types:
        group = df.groupby(by=["sb", "type", "subtype", "name"], observed=True)
    else:
        group = df.groupby(by=["sb", "name"], observed=True)

    df_analyzed = (
        group["qty"].agg(["count", "sum",
                        lambda x: x.mean().round(0).astype(int),
                        "mean", "std", "min", "max",
                        lambda x: (x==((x.mean().round(0))-2)).sum() / x.count(),
                        lambda x: (x==((x.mean().round(0))-1)).sum() / x.count(),
                        lambda x: (x==((x.mean().round(0))+0)).sum() / x.count(),
                        lambda x: (x==((x.mean().round(0))+1)).sum() / x.count(),
                        lambda x: (x==((x.mean().round(0))+2)).sum() / x.count(),
                        lambda x: _calculate_final_qty_cards(x, x.count()),
                    ])
                    .rename(columns={
                        'count': 'n_dls',
                        '<lambda_0>': 'mean_rnd',
                        '<lambda_1>': '% copies = mean-2',
                        '<lambda_2>': '% copies = mean-1',
                        '<lambda_3>': '% copies = mean',
                        '<lambda_4>': '% copies = mean+1',
                        '<lambda_5>': '% copies = mean+2',
                        '<lambda_6>': 'final_qty',
                    })
                    .sort_values(by=["sb", "type", "subtype", "sum", "name"] if types else ["sb", "sum", "name"],
                                 ascending=[True, True, True, False, True] if types else [True, False, True])
                    .reset_index()
    )

    df_analyzed["diff"] = df_analyzed["final_qty"] - df_analyzed["mean"]

    df_analyzed = _adjust_final_qty(df_analyzed)

    cols_ordered = df_analyzed.columns.to_list()
    cols_ordered.remove("name")
    cols_ordered.remove("final_qty")
    cols_ordered = cols_ordered[:1] + ["final_qty", "name"] + cols_ordered[1:]
    df_analyzed = df_analyzed[cols_ordered]

    return df_analyzed


def analyze_dls_types(df, subtypes=False):
    
    def _calculate_final_qty_types(x, n_decks):
        probabilities = [
            (x==((x.sum()/n_decks)).round(0)-2).sum() / n_decks,
            (x==((x.sum()/n_decks)).round(0)-1).sum() / n_decks,
            (x==((x.sum()/n_decks)).round(0)+0).sum() / n_decks,
            (x==((x.sum()/n_decks)).round(0)+1).sum() / n_decks,
            (x==((x.sum()/n_decks)).round(0)+2).sum() / n_decks,
        ]
        mean_diff = probabilities.index(max(probabilities)) - 2
        return (x.sum()/n_decks).round(0) + mean_diff

    if subtypes:
        group1 = df.groupby(by=["sb", "type", "subtype"], observed=True)
    else:
        group1 = df.groupby(by=["sb", "type"], observed=True)
    
    n_decks = df["#dl"].nunique()

    df1 = (
        group1["qty"].agg([lambda x: n_decks,
                          lambda x: int(x.sum()),
                          lambda x: (x.sum() / n_decks).round(0),
                          lambda x: x.sum() / n_decks,
                          lambda x: np.sqrt((((x - (x.sum() / n_decks))**2).sum())/n_decks),
                    ])
                    .rename(columns={
                          '<lambda_0>': 'n_dls',
                          '<lambda_1>': 'sum',
                          '<lambda_2>': 'mean_rnd',
                          '<lambda_3>': 'mean',
                          '<lambda_4>': 'std',
                    })
    )

    if subtypes:
        group2 = df.groupby(by=["#dl", "sb", "type", "subtype"], observed=True)["qty"].sum()
        group3 = group2.groupby(by=["sb", "type", "subtype"], observed=True)
    else:
        group2 = df.groupby(by=["#dl", "sb", "type"], observed=True)["qty"].sum()
        group3 = group2.groupby(by=["sb", "type"], observed=True)

    df2 = (group3.agg(["min", "max",
                      lambda x: (x==((x.sum()/n_decks)).round(0)-2).sum() / n_decks,
                      lambda x: (x==((x.sum()/n_decks)).round(0)-1).sum() / n_decks,
                      lambda x: (x==((x.sum()/n_decks)).round(0)+0).sum() / n_decks,
                      lambda x: (x==((x.sum()/n_decks)).round(0)+1).sum() / n_decks,
                      lambda x: (x==((x.sum()/n_decks)).round(0)+2).sum() / n_decks,
                      lambda x: _calculate_final_qty_types(x, n_decks),
                    ])
                .rename(columns={
                    '<lambda_0>': "% copies = mean-2",
                    '<lambda_1>': "% copies = mean-1",
                    '<lambda_2>': "% copies = mean",
                    '<lambda_3>': "% copies = mean+1",
                    '<lambda_4>': "% copies = mean+2",
                    '<lambda_5>': "final_qty",
                })
    )

    df3 = (
        pd.concat([df1, df2], axis=1)
          .sort_values(
              by=["sb", "type", "subtype", "sum"] if subtypes else ["sb", "type", "sum"],
              ascending=[True, True, True, False] if subtypes else [True, True, False]
          )
          .reset_index()
    )

    cols_ordered = df3.columns.to_list()
    cols_ordered.remove("final_qty")
    cols_ordered = cols_ordered[:1] + ["final_qty"] + cols_ordered[1:]
    df3 = df3[cols_ordered]

    return df3