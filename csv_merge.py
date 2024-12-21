import os
import json
from datetime import datetime
import pandas as pd
import numpy as np
from tqdm import tqdm
from utils import (
    load_bookmakers,
    load_bookmaker_data_for_match,
    process_outcomes,
    calculate_roi
)

min_roi = 5  # Минимальный ROI для фильтрации

def load_all_matched_events(path='matching/bookmaker_mappings'):
    all_events = []
    for bookmaker in os.listdir(path):
        bookmaker_path = os.path.join(path, bookmaker, 'matched_events.json')
        if os.path.exists(bookmaker_path):
            with open(bookmaker_path, 'r') as f:
                events = json.load(f)
            for event in events:
                event['bookmaker'] = bookmaker
            all_events.extend(events)
    return all_events

def find_closest_event(other_data, p_time, is_live):
    max_time_diff = 5 if is_live else 25  # 5 seconds for live, 25 seconds for pre-match
    closest_event = min(
        (event for event in other_data if abs(event['time'] - p_time) <= max_time_diff),
        key=lambda x: abs(x['time'] - p_time),
        default=None
    )
    return closest_event

def generate_match_csv(match_info, bookmakers):
    csv_data = []
    pinnacle_data = load_bookmaker_data_for_match('pinnacle', bookmakers, match_info['pinnacle_match_name'])
    if match_info['bookmaker'] not in bookmakers:
        print(
            f"Bookmaker {match_info['bookmaker']} not found in active bookmakers list. Skipping match: {match_info['pinnacle_match_name']}")
        return None
    other_data = load_bookmaker_data_for_match(
        match_info['bookmaker'], bookmakers, match_info['other_match_name'])
    if not pinnacle_data or not other_data:
        print(f"Data not found for match: {match_info['pinnacle_match_name']}")
        return None

    # Сортируем данные по времени
    pinnacle_data.sort(key=lambda x: x['time'])
    other_data.sort(key=lambda x: x['time'])

    for p_event in pinnacle_data:
        p_time = p_event['time']
        p_outcomes = process_outcomes(p_event.get('outcomes', []))

        # Определяем, является ли событие live
        is_live = p_event.get('type', '').lower() == 'live'

        # Находим ближайшее по времени событие другого букмекера с ограничением по времени
        closest_other_event = find_closest_event(other_data, p_time, is_live)
        if closest_other_event is None:
            continue  # Пропускаем, если не нашли подходящее событие

        o_outcomes = process_outcomes(closest_other_event.get('outcomes', []))

        row = {
            'Timestamp': datetime.fromtimestamp(p_time).strftime('%Y-%m-%d %H:%M:%S'),
        }

        for outcome_type in set(p_outcomes.keys()) | set(o_outcomes.keys()):
            p_odds = p_outcomes.get(outcome_type, '-')
            o_odds = o_outcomes.get(outcome_type, '-')
            if p_odds != '-' and o_odds != '-':
                roi = calculate_roi(p_odds, o_odds)
                row[f'Pinnacle {outcome_type}'] = p_odds
                row[f'{match_info["bookmaker"]} {outcome_type}'] = o_odds
                row[f'ROI {outcome_type}'] = round(roi, 2)
            else:
                row[f'Pinnacle {outcome_type}'] = p_odds
                row[f'{match_info["bookmaker"]} {outcome_type}'] = o_odds
                row[f'ROI {outcome_type}'] = '-'
        csv_data.append(row)
    return csv_data

def save_match_csvs(match_info, bookmakers, output_dir):
    csv_data = generate_match_csv(match_info, bookmakers)
    if csv_data:
        df = pd.DataFrame(csv_data)
        # Declare the order of columns
        columns_order = ['Timestamp']
        # Get list of unique outcome types
        outcome_types = sorted(set(
            col.split(' ', 1)[1] for col in df.columns if 'Pinnacle' in col))
        # Add columns in the desired order
        for outcome in outcome_types:
            columns_order.append(f'Pinnacle {outcome}')
            columns_order.append(f'{match_info["bookmaker"]} {outcome}')
            columns_order.append(f'ROI {outcome}')
        # Reorder columns
        df = df[columns_order]

        # Create filename
        file_name = f"{match_info['pinnacle_match_name']}_{match_info['pinnacle_league']}_{datetime.fromtimestamp(match_info['start_time']).strftime('%Y-%m-%d_%H-%M-%S')}_{match_info['bookmaker']}"

        # Save full table
        full_output_path = os.path.join(output_dir, f"{file_name}_full.csv")
        df.to_csv(full_output_path, index=False)

        # Create and save filtered table
        filtered_df = df.copy()

        # Process ROI values according to conditions
        for outcome in outcome_types:
            roi_col = f'ROI {outcome}'
            pinn_col = f'Pinnacle {outcome}'
            book_col = f'{match_info["bookmaker"]} {outcome}'

            # Convert data to numeric, coerce errors to NaN
            filtered_df[roi_col] = pd.to_numeric(filtered_df[roi_col], errors='coerce')
            filtered_df[pinn_col] = pd.to_numeric(filtered_df[pinn_col], errors='coerce')
            filtered_df[book_col] = pd.to_numeric(filtered_df[book_col], errors='coerce')

            # Create masks for conditions
            mask_pinnacle_odds_high = filtered_df[pinn_col] > 3.7
            mask_roi_low = filtered_df[roi_col] < min_roi

            # Set NaN in ROI where conditions are met
            filtered_df.loc[mask_pinnacle_odds_high | mask_roi_low, roi_col] = np.nan

            # Set corresponding odds to NaN where ROI is NaN
            mask_roi_nan = filtered_df[roi_col].isna()
            filtered_df.loc[mask_roi_nan, pinn_col] = np.nan
            filtered_df.loc[mask_roi_nan, book_col] = np.nan

        # Drop columns where all ROI values are NaN
        roi_columns = [col for col in filtered_df.columns if col.startswith('ROI')]
        for roi_col in roi_columns:
            if filtered_df[roi_col].isna().all():
                # Also drop corresponding Pinnacle and bookmaker columns
                outcome = roi_col.split(' ', 1)[1]
                pinn_col = f'Pinnacle {outcome}'
                book_col = f'{match_info["bookmaker"]} {outcome}'
                filtered_df.drop(columns=[roi_col, pinn_col, book_col], inplace=True)

        # Update list of columns to keep
        columns_to_keep = ['Timestamp'] + [col for col in filtered_df.columns if col != 'Timestamp']
        filtered_df = filtered_df[columns_to_keep]

        # Drop rows where all ROI values are NaN
        roi_columns = [col for col in filtered_df.columns if col.startswith('ROI')]
        if roi_columns:
            mask_all_roi_nan = filtered_df[roi_columns].isna().all(axis=1)
            filtered_df = filtered_df[~mask_all_roi_nan]

        # Check if there is data to save
        if filtered_df.empty or len(filtered_df.columns) <= 1:
            print(f"No valid data after filtering for match: {match_info['pinnacle_match_name']}")
            return  # Do not save file if no data left

        # Save filtered DataFrame to CSV with NaN represented as '-'
        filtered_output_path = os.path.join(output_dir, f"{file_name}_filtered.csv")
        filtered_df.to_csv(filtered_output_path, index=False, na_rep='-')
        print(f"CSVs saved for match: {match_info['pinnacle_match_name']}")
    else:
        print(f"Failed to generate CSVs for match: {match_info['pinnacle_match_name']}")

from concurrent.futures import ProcessPoolExecutor
from functools import partial

def main():
    config_path = 'bookmakers.json'
    bookmakers = load_bookmakers(config_path)
    matched_events = load_all_matched_events()
    output_dir = 'match_csv_files'
    os.makedirs(output_dir, exist_ok=True)

    # Оборачиваем функцию с фиксированными аргументами
    process_match = partial(save_match_csvs, bookmakers=bookmakers, output_dir=output_dir)

    with ProcessPoolExecutor() as executor:
        list(tqdm(executor.map(process_match, matched_events), total=len(matched_events), desc="Processing matches"))

if __name__ == "__main__":
    main()