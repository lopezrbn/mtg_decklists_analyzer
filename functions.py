import os
import pandas as pd
import numpy as np
import json
import requests
from bs4 import BeautifulSoup
import re


base_path = r"J:\Mi unidad\1 - Data Science\Proyectos personales\Decklist analysis\Decklists"


def download_decklists(deck_name, n_pages):
    
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
    print("\n\tDone!\n")
    
    ### Download decklists for each deck id
    print(f"Downloading decklists...")
    decklists = []
    for i, (id_, iddeck) in enumerate(decks_ids):
        print(f"\r\tFetching {i+1}/{len(decks_ids)}: id {id_}, iddeck {iddeck}...", end="")
        url = f"https://www.tcdecks.net/download.php?ext=txt&id={id_}&iddeck={iddeck}"
        r = requests.get(url)
        decklists.append(r.text)
    print("\n\tDone!\n")

    ### Save decklists to txt files
    full_path = os.path.join(base_path, deck_name.replace("%20", " "))
    os.makedirs(full_path, exist_ok=True)
    print(f"Saving decklists to '{full_path}'...")
    for i, decklist in enumerate(decklists):
        print(f"\r\tSaving decklist {i+1}/{len(decks_ids)}...", end="")
        final_path = os.path.join(full_path, f'decklist_{i+1}.txt')
        with open(final_path, 'w') as f:
            f.write(decklist.replace('\n', ''))
    print("\n\tDone!\n")


def read_decklists(deck_name):
    path = os.path.join(base_path, deck_name)
    df = pd.DataFrame()
    for file in os.listdir(path):
        # Check whether file is in text format or not
        if file.endswith(".txt"):
            file_path = os.path.join(path, file)
            df = pd.concat([df, pd.read_csv(file_path, header=None, skip_blank_lines=False, sep="*")], axis=1)
    df.columns = list(range(len(df.columns)))
    return df


def process_decklists(df):
    
    def read_cards_db(path):
        df_db = pd.read_excel(path)
        db_dict = {}
        for row in df_db.iterrows():
            row_list = row[1].values
            if row_list[0] not in db_dict:
                db_dict[row_list[0]] = {}
            if row_list[1] not in db_dict[row_list[0]]:
                db_dict[row_list[0]][row_list[1]] = {}
            if row_list[2] not in db_dict[row_list[0]][row_list[1]]:
                db_dict[row_list[0]][row_list[1]][row_list[2]] = {}
            db_dict[row_list[0]][row_list[1]][row_list[2]]["type"] = row_list[3]
            db_dict[row_list[0]][row_list[1]][row_list[2]]["subtype"] = row_list[4]
        return db_dict

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
    df_main['qty'] = df_main['qty'].fillna(0)
    df_sideboard['qty'] = df_sideboard['qty'].fillna(0)
    # Concatenate main and sideboard
    df = pd.concat([df_main, df_sideboard])
    

    #### Include card type

    # Read cards database
    # with open("cards_database.json", "r") as file:
    #     cards_database = json.load(file)
    cards_database = read_cards_db("cards_database_landstill.xlsx")

    # Include card type
    df["type"] = df["name"].map(cards_database["Premodern"]["Landstill"]).str["type"]
    df["subtype"] = df["name"].map(cards_database["Premodern"]["Landstill"]).str["subtype"]
    # df["subtype"] = df["name"].map(cards_database).str["subtype"]
        
    
    # Reorder columns
    df = df[["#dl", "sb", "type", "subtype", "qty", "name"]]
    # Sort by decklist number and sb
    df = df.sort_values(by=["#dl", "sb", "type", "subtype", "qty", "name"], ascending=[True, True, True, True, False, True])
    # Reset index
    df = df.reset_index(drop=True)

    return df


def analyze_dls_cards(df, types=False):

    if types:
        group = df.groupby(by=["sb", "type", "subtype", "name"])
    else:
        group = df.groupby(by=["sb", "name"])

    df_analyzed = (
        group["qty"].agg(["count", "sum",
                        lambda x: x.mean().round(0),
                        "mean", "std", "min",
                        lambda x: x.quantile(0.25),
                        lambda x: x.quantile(0.50),
                        lambda x: x.quantile(0.75),
                        "max"])
                    .rename(columns={
                        'count': 'n_decklists',
                        '<lambda_0>': 'mean_rnd',
                        '<lambda_1>': '25%',
                        '<lambda_2>': '50%',
                        '<lambda_3>': '75%'})
                    .sort_values(by=["sb", "type", "subtype", "sum", "name"] if types else ["sb", "sum", "name"],
                                 ascending=[True, True, True, False, True] if types else [True, False, True])
                    .reset_index()
    )

    return df_analyzed


def analyze_dls_types(df, subtypes=False):
    
    if subtypes:
        group1 = df.groupby(by=["sb", "type", "subtype"])
    else:
        group1 = df.groupby(by=["sb", "type"])
    
    n_decks = df["#dl"].nunique()

    df1 = (
        group1["qty"].agg([lambda x: n_decks,
                          lambda x: int(x.sum()),
                          lambda x: (x.sum() / n_decks).round(0),
                          lambda x: x.sum() / n_decks,
                          lambda x: np.sqrt((((x - (x.sum() / n_decks))**2).sum())/n_decks),
                    ])
                    .rename(columns={
                          '<lambda_0>': 'n_decklists',
                          '<lambda_1>': 'sum',
                          '<lambda_2>': 'mean_rnd',
                          '<lambda_3>': 'mean',
                          '<lambda_4>': 'std',
                    })
    )

    if subtypes:
        group2 = df.groupby(by=["#dl", "sb", "type", "subtype"])["qty"].sum()
        group3 = group2.groupby(by=["sb", "type", "subtype"])
    else:
        group2 = df.groupby(by=["#dl", "sb", "type"])["qty"].sum()
        group3 = group2.groupby(by=["sb", "type"])

    df2 = group3.agg(["min", "max"])

    df3 = (
        pd.concat([df1, df2], axis=1)
          .sort_values(
              by=["sb", "type", "subtype", "sum"] if subtypes else ["sb", "type", "sum"],
              ascending=[True, True, True, False] if subtypes else [True, True, False]
          )
          .reset_index()
    )

    return df3