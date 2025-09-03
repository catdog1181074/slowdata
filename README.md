# SLOW Token On-Chain Analysis

This repo contains scripts and data used to investigate the **$SLOW KRC20 token** on Kaspa, including minter distributions, funder relationships, and potential clustering of wallets.

---

##  Contents

- **`trace_kaspa_fullhistory.py`**  
  Python script to download **full Kaspa L1 transaction history** for a given wallet.  
  Saves both *all participant* transactions and *filtered (involving only the wallet)* transactions into CSV format.

- **`tracekrc20_kasplex_full.py`**  
  Python script to download **KRC20 token transaction history** (mint, transfer, list, etc.) using the **Kasplex API**.  
  Supports pagination, retries, and outputs clean CSVs.

- **`slow_token_ops.csv`**  
  Normalized CSV of **all $SLOW token operations** (mint, transfers, listings, burns).  
  This is the primary dataset used for distribution and wash-trading analyses.

- **`flow_data_full.zip`**  
  Zipped collection of **Kaspa L1 transaction histories** for the top $SLOW wallets and related funders.  
  Useful for analyzing cross-wallet funding and potential common control.

---

## ⚡ Usage

### Download KRC20 Token History
```
python tracekrc20_kasplex_full.py --tick SLOW
```

### Download Kaspa L1 Full History
```
python trace_kaspa_fullhistory.py 
```

Both scripts save results into CSVs for further analysis (e.g. using pandas, networkx, or visualization tools).

---

## Notes

- The repo is intended for transparency—others can rerun the scripts, re-check the data, and contribute improvements.
