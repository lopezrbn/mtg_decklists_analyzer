import os
import pandas as pd
import numpy as np
import json
import requests
from bs4 import BeautifulSoup
import re


base_path = os.path.join("0_data", "decklists")


def download_decklists(deck_name, format, n_pages):
    
    deck_name = deck_name.lower().replace(" ", "_")
    format = format.lower().replace(" ", "_")

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
    format = format.lower().replace(" ", "_")
    deck_name = deck_name.lower().replace(" ", "_")
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
    
    format = format.lower().replace(" ", "_")

    def _read_cards_db(name_values:pd.Series,
                       cards_db_excel_path=os.path.join("0_data", "cards_database.xlsx"),
                       cards_db_json_path=os.path.join("0_data", "cards_database.json")):

        def _read_n_update_cards_db(name_values:pd.Series,
                                    cards_db_json_path=os.path.join("0_data", "cards_database.json")):
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
    

    def _create_deck_colors_column(df):
        """Create a column with the colors of the deck"""

        def _join_colors(group):
            """"Define a function that joins all the 'color' strings in a group, removes 'C', and sorts the characters"""
            colors = ''.join(group['color'].fillna('').astype(str))
            # Remove 'C'
            colors = colors.replace('C', '')
            # Sort in the order 'WUBRG' and remove duplicates
            colors = ''.join(sorted(set(c for c in colors if c in "WUBRG"), key="WUBRG".index))
            return colors

        # Filter the DataFrame to only include rows where 'qty' > 0
        df_filtered = df[(df['qty'] > 0) & (df['type'] != "Land")]
        # Create a DataFrame with the colors of each deck
        deck_colors = df_filtered.groupby('#dl').apply(_join_colors).reset_index()
        deck_colors.columns = ['#dl', 'deck_colors']
        # Merge df with deck_colors
        df = df.merge(deck_colors, on='#dl', how='left')
        # Reorder the characters in 'deck_colors' to be in the order 'WUBRG'
        df['deck_colors'] = df['deck_colors'].apply(lambda x: ''.join(sorted(x, key="WUBRG".index)))
        # Reset the index
        df.reset_index(inplace=True, drop=True)

        return df["deck_colors"]

    def _fill_all_possible_cards(df):
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
        return df


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
    
    # Read, update and populate cards database
    cards_database = _read_cards_db(name_values=df["name"],
                                    cards_db_excel_path=os.path.join("0_data", "cards_database.xlsx"),
                                    cards_db_json_path=os.path.join("0_data", "cards_database.json"))
    
    # Create column type with the types of the cards
    df["type"] = df["name"].map(cards_database[format]).str["type"]
    # Create column color with the colors of the cards
    df["color"] = df["name"].map(cards_database[format]).str["color"]
    # Create a column with the colors of the deck
    df["deck_colors"] = _create_deck_colors_column(df)

    dfs = []
    for deck_color in df["deck_colors"].unique():
        # Filter by deck color
        df_color = df[df["deck_colors"]==deck_color]
        # Fill missing cards to have all the possible cards in every decklist
        df_color = _fill_all_possible_cards(df_color)
        # Repopulate 'deck_colors', 'color' and 'type' columns after filling missing cards
        df_color["deck_colors"] = deck_color
        df_color["color"] = df_color["name"].map(cards_database[format]).str["color"]
        df_color["type"] = df_color["name"].map(cards_database[format]).str["type"]
        # Create column subtype
        df_color["subtype"] = df_color["name"].map(cards_database[format]).str["subtype"]  
        # Reorder columns
        df_color = df_color[["#dl", "deck_colors", "sb", "type", "subtype", "color", "qty", "name"]]
        # Sort by decklist number and sb
        df_color = df_color.sort_values(by=["#dl", "sb", "type", "subtype", "qty", "color", "name"], ascending=[True, True, True, True, False, True, True])
        # Reset index
        df_color = df_color.reset_index(drop=True)
        # Append to list
        dfs.append(df_color)

    return dfs


def analyze_dls(df, types=False):

    def _analyze_dls_types(df, subtypes=False):
        
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
                        ])
                        .rename(columns={
                            '<lambda_0>': 'n_dls',
                            '<lambda_1>': 'sum',
                            '<lambda_2>': 'mean_rnd',
                            '<lambda_3>': 'mean',
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




    def _analyze_dls_cards(df, types=False, types_list=None):

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

        
        def _adjust_final_qty(df_orig, types_list):
            dfs = []
            for sb, type, qty in types_list:
                df = df_orig[(df_orig["sb"]==sb) & (df_orig["type"]==type)].sort_values(by=["%_mode_2nd"], ascending=False)
                indices = df["%_mode_2nd"].index
                adj_qty = df["final_qty"].sum()
                target = qty
                iterations = 0
                while (adj_qty != target) & (iterations < 1000):
                    iterations += 1
                    if adj_qty > target:
                        for index in indices:
                            if df.loc[index, "mode_2nd"] < df.loc[index, "final_qty"]:
                                df.loc[index, "final_qty"] = df.loc[index, "mode_2nd"]
                                adj_qty = df["final_qty"].sum()
                                break
                    else:
                        for index in indices:
                            if df.loc[index, "mode_2nd"] > df.loc[index, "final_qty"]:
                                df.loc[index, "final_qty"] = df.loc[index, "mode_2nd"]
                                adj_qty = df["final_qty"].sum()
                                break
                dfs.append(df)
            df = pd.concat(dfs)
            return df


        if types:
            group1 = df.groupby(by=["sb", "type", "subtype", "name"], observed=True)
        else:
            group1 = df.groupby(by=["sb", "name"], observed=True)

        df1 = (
            group1["qty"].agg(["count", "sum",
                            lambda x: x.mean().round(0).astype(int),
                            "mean", "std",
                            lambda x: x.value_counts().index[0] if len(x.value_counts()) > 0 else np.nan,
                            lambda x: (x==(x.value_counts().index[0] if len(x.value_counts()) > 0 else np.nan)).sum() / df["#dl"].nunique(),
                            lambda x: x.value_counts().index[1] if len(x.value_counts()) > 1 else np.nan,
                            lambda x: (x==(x.value_counts().index[1] if len(x.value_counts()) > 1 else np.nan)).sum() / df["#dl"].nunique(),
                            lambda x: x.value_counts().index[2] if len(x.value_counts()) > 2 else np.nan,
                            lambda x: (x==(x.value_counts().index[2] if len(x.value_counts()) > 2 else np.nan)).sum() / df["#dl"].nunique(),
                            lambda x: x.value_counts().index[3] if len(x.value_counts()) > 3 else np.nan,
                            lambda x: (x==(x.value_counts().index[3] if len(x.value_counts()) > 3 else np.nan)).sum() / df["#dl"].nunique(),
                            lambda x: x.value_counts().index[4] if len(x.value_counts()) > 4 else np.nan,
                            lambda x: (x==(x.value_counts().index[4] if len(x.value_counts()) > 4 else np.nan)).sum() / df["#dl"].nunique(),
                            "min", "max",
                            lambda x: _calculate_final_qty_cards(x, x.count()),
                        ])
                        .rename(columns={
                            'count': 'n_dls',
                            '<lambda_0>': 'mean_rnd',
                            '<lambda_1>': 'mode_1st',
                            '<lambda_2>': '%_mode_1st',
                            '<lambda_3>': 'mode_2nd',
                            '<lambda_4>': '%_mode_2nd',
                            '<lambda_5>': 'mode_3rd',
                            '<lambda_6>': '%_mode_3rd',
                            '<lambda_7>': 'mode_4th',
                            '<lambda_8>': '%_mode_4th',
                            '<lambda_9>': 'mode_5th',
                            '<lambda_10>': '%_mode_5th',
                            '<lambda_11>': 'final_qty',
                        })
                        # .sort_values(by=["sb", "type", "subtype", "sum", "name"] if types else ["sb", "sum", "name"],
                        #             ascending=[True, True, True, False, True] if types else [True, False, True])
                        .reset_index()
        )

        if types:
            group2 = df.groupby(by=["sb", "type", "subtype", "name"], observed=True)
        else:
            group2 = df.groupby(by=["sb", "name"], observed=True)

        df2 = (
            group2.apply(lambda x: (x["qty"]>0).sum() / x["#dl"].nunique())
                .reset_index()
                .rename(columns={0: "%_dls_w_card"})
                )

        df3 = df1.merge(df2, on=["sb", "type", "subtype", "name"] if types else ["sb", "name"], how="left")
        df3 = df3.sort_values(by=["sb", "type", "subtype", "sum", "name"] if types else ["sb", "sum", "name"],
                                ascending=[True, True, True, False, True] if types else [True, False, True])
        df3 = df3.reset_index(drop=True)

        df3["deck_colors"] = df["deck_colors"].unique()[0]
        df3["diff"] = df3["final_qty"] - df3["mean"]

        df3["final_qty"] = df3["mode_1st"]

        df3 = _adjust_final_qty(df3, types_list)

        cols_ordered = df3.columns.to_list()
        cols_ordered.remove("name")
        cols_ordered.remove("final_qty")
        cols_ordered.remove("deck_colors")
        cols_ordered.remove("%_dls_w_card")
        cols_ordered = ["deck_colors"] + cols_ordered[:1] + ["final_qty", "name"] +cols_ordered[1:8] + ["%_dls_w_card"] + cols_ordered[8:]
        # cols_ordered = ["deck_colors"] + cols_ordered[:1] + ["final_qty", "name"] + cols_ordered[1:]
        df3 = df3[cols_ordered]
        df3 = df3.sort_values(by=["sb", "type", "subtype", "sum", "name"] if types else ["sb", "sum", "name"],
                                ascending=[True, True, True, False, True] if types else [True, False, True])

        return df3


    df_types = _analyze_dls_types(df, subtypes=False)
    sb_results, types_results, types_qty_results = df_types["sb"], df_types["type"], df_types["final_qty"]
    types_results_list = list(zip(sb_results, types_results, types_qty_results))
    df_cards = _analyze_dls_cards(df, types=True, types_list=types_results_list)

    return df_types, df_cards


