"""
SOG Entity Resolution Testing Example
======================================

This script demonstrates how to use SOG synthetic data to test 
an entity resolution (record linkage) algorithm.

Prerequisites:
- Run: python scripts/run_phase2_pipeline.py --scenario couple_merge
  (or use an existing run like 2026-03-11_couple_merge_seed20260311)

What this script does:
1. Loads Dataset A and Dataset B (the "messy" observed data)
2. Loads the ground truth crosswalk (the "answer key")
3. Runs a simple matching algorithm (replace with yours!)
4. Evaluates precision, recall, and F1 score
5. Shows examples of false positives and false negatives
"""

import pandas as pd
import json
from pathlib import Path
from typing import Tuple, Set


PROJECT_ROOT = Path(__file__).resolve().parents[1]

# ============================================================================
# CONFIGURATION - Change this to use a different run
# ============================================================================

RUN_ID = "2026-03-11_couple_merge_seed20260311"  # Existing run with couple formation
# RUN_ID = "2026-03-10_single_movers_seed20260310"  # Alternative: people moving
RUN_DIR = PROJECT_ROOT / "phase2" / "runs" / RUN_ID

# ============================================================================
# DATA LOADING
# ============================================================================

def load_datasets() -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Load the synthetic datasets and ground truth.
    
    Returns:
        dataset_a: The first "messy" dataset (CSV)
        dataset_b: The second "messier" dataset (CSV)
        crosswalk: Ground truth mapping between A and B (CSV)
    """
    if not RUN_DIR.exists():
        raise FileNotFoundError(
            f"Run directory not found: {RUN_DIR}\n"
            f"Please run: python scripts/run_phase2_pipeline.py --scenario couple_merge"
        )
    
    dataset_a = pd.read_csv(RUN_DIR / "DatasetA.csv")
    dataset_b = pd.read_csv(RUN_DIR / "DatasetB.csv")
    crosswalk = pd.read_csv(RUN_DIR / "truth_crosswalk.csv")
    
    return dataset_a, dataset_b, crosswalk


def load_truth_events() -> pd.DataFrame:
    """Load the ground truth life events (COHABIT, MOVE, etc.)"""
    return pd.read_parquet(RUN_DIR / "truth_events.parquet")


# ============================================================================
# ENTITY RESOLUTION ALGORITHMS (Replace these with your own!)
# ============================================================================

def simple_exact_match(dataset_a: pd.DataFrame, dataset_b: pd.DataFrame) -> pd.DataFrame:
    """
    SIMPLE ALGORITHM: Exact match on LastName + DOB
    
    This is a naive baseline algorithm. In practice, you'd use:
    - Fuzzy string matching on names
    - Probabilistic record linkage (Fellegi-Sunter)
    - Machine learning models
    - Blocking + similarity thresholds
    """
    matches = []
    
    # Create lookup index for dataset_b
    b_index = {}
    for _, row_b in dataset_b.iterrows():
        key = (row_b['LastName'], row_b['DOB'])
        if key not in b_index:
            b_index[key] = []
        b_index[key].append(row_b['B_RecordKey'])
    
    # Find matches
    for _, row_a in dataset_a.iterrows():
        key = (row_a['LastName'], row_a['DOB'])
        if key in b_index:
            for b_record_key in b_index[key]:
                matches.append({
                    'A_RecordKey': row_a['A_RecordKey'],
                    'B_RecordKey': b_record_key,
                    'match_method': 'exact_lastname_dob'
                })
    
    return pd.DataFrame(matches)


def fuzzy_name_match(dataset_a: pd.DataFrame, dataset_b: pd.DataFrame) -> pd.DataFrame:
    """
    SLIGHTLY BETTER: Match on DOB + fuzzy last name (first 3 chars)
    
    This handles minor name spelling variations.
    """
    matches = []
    
    # Create lookup index on DOB + last name prefix
    b_index = {}
    for _, row_b in dataset_b.iterrows():
        key = (row_b['DOB'], str(row_b['LastName'])[:3].upper())
        if key not in b_index:
            b_index[key] = []
        b_index[key].append(row_b['B_RecordKey'])
    
    for _, row_a in dataset_a.iterrows():
        key = (row_a['DOB'], str(row_a['LastName'])[:3].upper())
        if key in b_index:
            for b_record_key in b_index[key]:
                matches.append({
                    'A_RecordKey': row_a['A_RecordKey'],
                    'B_RecordKey': b_record_key,
                    'match_method': 'fuzzy_dob_lastname3'
                })
    
    return pd.DataFrame(matches)


# ============================================================================
# EVALUATION
# ============================================================================

def evaluate_matches(
    predicted: pd.DataFrame, 
    truth: pd.DataFrame
) -> dict:
    """
    Evaluate predicted matches against ground truth.
    
    Returns precision, recall, F1, and confusion matrix counts.
    """
    # Convert to sets of tuples for easy comparison
    pred_set: Set[Tuple[str, str]] = set(
        zip(predicted['A_RecordKey'], predicted['B_RecordKey'])
    )
    
    true_matches = truth[truth['A_RecordKey'].notna() & truth['B_RecordKey'].notna()]
    truth_set: Set[Tuple[str, str]] = set(
        zip(true_matches['A_RecordKey'], true_matches['B_RecordKey'])
    )
    
    # Calculate confusion matrix
    true_positives = len(pred_set & truth_set)
    false_positives = len(pred_set - truth_set)
    false_negatives = len(truth_set - pred_set)
    
    # Calculate metrics
    precision = true_positives / (true_positives + false_positives) if (true_positives + false_positives) > 0 else 0
    recall = true_positives / (true_positives + false_negatives) if (true_positives + false_negatives) > 0 else 0
    f1 = 2 * (precision * recall) / (precision + recall) if (precision + recall) > 0 else 0
    
    return {
        'precision': precision,
        'recall': recall,
        'f1_score': f1,
        'true_positives': true_positives,
        'false_positives': false_positives,
        'false_negatives': false_negatives,
        'total_predicted': len(pred_set),
        'total_truth': len(truth_set)
    }


def print_error_examples(
    predicted: pd.DataFrame,
    truth: pd.DataFrame,
    dataset_a: pd.DataFrame,
    dataset_b: pd.DataFrame,
    n_examples: int = 3
):
    """Print examples of false positives and false negatives for analysis."""
    
    pred_set = set(zip(predicted['A_RecordKey'], predicted['B_RecordKey']))
    true_matches = truth[truth['A_RecordKey'].notna() & truth['B_RecordKey'].notna()]
    truth_set = set(zip(true_matches['A_RecordKey'], true_matches['B_RecordKey']))
    
    # Create lookup dictionaries
    a_lookup = dataset_a.set_index('A_RecordKey').to_dict('index')
    b_lookup = dataset_b.set_index('B_RecordKey').to_dict('index')
    
    # False Positives (wrong matches)
    false_positives = list(pred_set - truth_set)[:n_examples]
    
    if false_positives:
        print(f"\n{'='*70}")
        print("FALSE POSITIVES (Incorrectly Matched)")
        print(f"{'='*70}")
        for a_key, b_key in false_positives:
            a_rec = a_lookup.get(a_key, {})
            b_rec = b_lookup.get(b_key, {})
            print(f"\n  A ({a_key}): {a_rec.get('FirstName')} {a_rec.get('LastName')}, "
                  f"DOB: {a_rec.get('DOB')}, Addr: {a_rec.get('AddressKey')}")
            print(f"  B ({b_key}): {b_rec.get('FirstName')} {b_rec.get('LastName')}, "
                  f"DOB: {b_rec.get('DOB')}, Addr: {b_rec.get('AddressKey')}")
            print(f"  -> Why wrong? Same last name by coincidence? Typo in DOB?")
    
    # False Negatives (missed matches)
    false_negatives = list(truth_set - pred_set)[:n_examples]
    
    if false_negatives:
        print(f"\n{'='*70}")
        print("FALSE NEGATIVES (Missed Matches)")
        print(f"{'='*70}")
        for a_key, b_key in false_negatives:
            a_rec = a_lookup.get(a_key, {})
            b_rec = b_lookup.get(b_key, {})
            print(f"\n  A ({a_key}): {a_rec.get('FirstName')} {a_rec.get('LastName')}, "
                  f"DOB: {a_rec.get('DOB')}, Addr: {a_rec.get('AddressKey')}")
            print(f"  B ({b_key}): {b_rec.get('FirstName')} {b_rec.get('LastName')}, "
                  f"DOB: {b_rec.get('DOB')}, Addr: {b_rec.get('AddressKey')}")
            
            # Analyze why it was missed
            reasons = []
            if a_rec.get('LastName') != b_rec.get('LastName'):
                reasons.append("Last names differ")
            if a_rec.get('DOB') != b_rec.get('DOB'):
                reasons.append("DOBs differ (shifted?)")
            if a_rec.get('AddressKey') != b_rec.get('AddressKey'):
                reasons.append("Addresses differ (moved?)")
            if not a_rec.get('MiddleName') or not b_rec.get('MiddleName'):
                reasons.append("Missing middle name")
            
            print(f"  -> Why missed? {'; '.join(reasons) if reasons else 'Check data quality'}")


# ============================================================================
# MAIN EXECUTION
# ============================================================================

def main():
    print("="*70)
    print("SOG Entity Resolution Testing Example")
    print("="*70)
    print(f"\nUsing run: {RUN_ID}")
    
    # ------------------------------------------------------------------
    # Step 1: Load Data
    # ------------------------------------------------------------------
    print("\n[1/5] Loading synthetic datasets...")
    try:
        dataset_a, dataset_b, crosswalk = load_datasets()
    except FileNotFoundError as e:
        print(f"\nError: {e}")
        print("\nTo fix this, run the pipeline first:")
        print("  python scripts/run_phase2_pipeline.py --scenario couple_merge")
        return
    
    print(f"  Dataset A: {len(dataset_a):,} records")
    print(f"  Dataset B: {len(dataset_b):,} records")
    
    # Crosswalk structure: PersonKey, A_RecordKey, B_RecordKey
    # True match: both A_RecordKey and B_RecordKey present
    # Unmatched A: only A_RecordKey present
    # Unmatched B: only B_RecordKey present
    true_matches = crosswalk[crosswalk['A_RecordKey'].notna() & crosswalk['B_RecordKey'].notna()]
    unmatched_a = crosswalk[crosswalk['A_RecordKey'].notna() & crosswalk['B_RecordKey'].isna()]
    unmatched_b = crosswalk[crosswalk['A_RecordKey'].isna() & crosswalk['B_RecordKey'].notna()]
    
    print(f"  Ground truth: {len(true_matches):,} true matches")
    print(f"                {len(unmatched_a):,} records only in A")
    print(f"                {len(unmatched_b):,} records only in B")
    
    # ------------------------------------------------------------------
    # Step 2: Run Algorithms
    # ------------------------------------------------------------------
    print("\n[2/5] Running entity resolution algorithms...")
    
    print("  -> Running simple exact match (LastName + DOB)...")
    predicted_simple = simple_exact_match(dataset_a, dataset_b)
    print(f"     Found {len(predicted_simple)} potential matches")
    
    print("  -> Running fuzzy match (DOB + LastName prefix)...")
    predicted_fuzzy = fuzzy_name_match(dataset_a, dataset_b)
    print(f"     Found {len(predicted_fuzzy)} potential matches")
    
    # ------------------------------------------------------------------
    # Step 3: Evaluate
    # ------------------------------------------------------------------
    print("\n[3/5] Evaluating against ground truth...")
    
    results_simple = evaluate_matches(predicted_simple, crosswalk)
    results_fuzzy = evaluate_matches(predicted_fuzzy, crosswalk)
    
    # ------------------------------------------------------------------
    # Step 4: Print Results
    # ------------------------------------------------------------------
    print("\n[4/5] Results:")
    print("-"*70)
    print(f"{'Algorithm':<25} {'Precision':>10} {'Recall':>10} {'F1':>10}")
    print("-"*70)
    print(f"{'Simple Exact Match':<25} "
          f"{results_simple['precision']:>10.3f} "
          f"{results_simple['recall']:>10.3f} "
          f"{results_simple['f1_score']:>10.3f}")
    print(f"{'Fuzzy Name Match':<25} "
          f"{results_fuzzy['precision']:>10.3f} "
          f"{results_fuzzy['recall']:>10.3f} "
          f"{results_fuzzy['f1_score']:>10.3f}")
    print("-"*70)
    
    # Detailed breakdown for best algorithm
    best = results_fuzzy if results_fuzzy['f1_score'] > results_simple['f1_score'] else results_simple
    best_name = 'Fuzzy Match' if best == results_fuzzy else 'Simple Match'
    best_pred = predicted_fuzzy if best == results_fuzzy else predicted_simple
    
    print(f"\n{best_name} Details:")
    print(f"  True Positives:  {best['true_positives']:,} (correctly matched)")
    print(f"  False Positives: {best['false_positives']:,} (wrongly matched)")
    print(f"  False Negatives: {best['false_negatives']:,} (missed matches)")
    
    # ------------------------------------------------------------------
    # Step 5: Error Analysis
    # ------------------------------------------------------------------
    print("\n[5/5] Error Analysis...")
    print_error_examples(best_pred, crosswalk, dataset_a, dataset_b, n_examples=3)
    
    # ------------------------------------------------------------------
    # Step 6: Show What Truth Events Look Like
    # ------------------------------------------------------------------
    print(f"\n{'='*70}")
    print("GROUND TRUTH LIFE EVENTS (Why records changed)")
    print(f"{'='*70}")
    
    events = load_truth_events()
    
    # Show COHABIT events (couples moving in together)
    cohabits = events[events['EventType'] == 'COHABIT']
    if len(cohabits) > 0:
        print(f"\nExample COHABIT events (couples forming households):")
        for _, event in cohabits.head(3).iterrows():
            print(f"  {event['EventDate']}: {event['PersonKeyA']} + {event['PersonKeyB']} "
                  f"(mode: {event['CohabitMode']})")
    
    # Show MOVE events
    moves = events[events['EventType'] == 'MOVE']
    if len(moves) > 0:
        print(f"\nExample MOVE events (address changes):")
        for _, event in moves.head(3).iterrows():
            if pd.notna(event.get('SubjectPersonKey')):
                print(f"  {event['EventDate']}: Person {event['SubjectPersonKey']} moved "
                      f"{event['FromAddressKey']} -> {event['ToAddressKey']}")
            else:
                print(f"  {event['EventDate']}: Household {event['SubjectHouseholdKey']} moved "
                      f"{event['FromAddressKey']} -> {event['ToAddressKey']}")
    
    print(f"\n{'='*70}")
    print("NEXT STEPS")
    print(f"{'='*70}")
    print("""
1. Try different scenarios:
   python scripts/run_phase2_pipeline.py --scenario single_movers
   python scripts/run_phase2_pipeline.py --scenario family_birth
   python scripts/run_phase2_pipeline.py --scenario divorce_custody

2. Modify the matching algorithms in this script to improve F1 score

3. Create a custom scenario for your specific use case

4. Load the truth events to understand WHY matches are difficult:
   - Address changes after MOVE events
   - Name variations after COHABIT (married names)
   - Missing data due to emission noise
""")


if __name__ == "__main__":
    main()
