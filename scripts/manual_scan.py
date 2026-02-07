
import asyncio
import logging
import os
import sys
from datetime import datetime

# Add project root to path
sys.path.append(os.getcwd())

# Ensure we can find the modules
from dotenv import load_dotenv

from core.dhan_bridge import DhanBridge

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger('ManualScan')

async def run_scan():
    load_dotenv()
    bridge = DhanBridge()
    # Strategy object not strictly needed for this simple scan logic as we calculate manually here

    # Load Watchlist from CSV
    try:
        import polars as pl
        watchlist_path = 'cache/fno_watchlist.csv'
        if not os.path.exists(watchlist_path):
            logger.error(f"Watchlist not found at {watchlist_path}")
            return
        
        df = pl.read_csv(watchlist_path)
        # Assuming column name is 'symbol' or 'trading_symbol' based on typical schema
        # Let's check first row to be safe or try both
        cols = df.columns
        sym_col = 'symbol' if 'symbol' in cols else 'trading_symbol'
        if sym_col not in cols:
            # Fallback if neither found (maybe it has 'Symbol'?)
            sym_col = cols[0] 
        
        target_names = df[sym_col].to_list()
        # Clean symbols just in case
        target_names = [s.strip().upper() for s in target_names if isinstance(s, str)]
        
        # Remove Indices if requested (NIFTY/BANKNIFTY) - User asked to exclude them earlier
        # But this request said "all stocks inside fno_watchlist". I will keep them if they are in there,
        # but sort logic will push them down if they diverge. 
        # Actually user said "excluding Nifty and BN", I should probably stick to that preference?
        # "Ok give me calculation of all stocks inside fno_watchlist" -> suggests ALL.
        # I'll include all but mark indices clearly.
    except Exception as e:
        logger.error(f"Failed to load watchlist: {e}")
        return
    
    results = []

    logger.info(f"🔍 Scanning {len(target_names)} instruments for Global Sentiment...")
    print("-" * 60)
    print(f"{'SYMBOL':<15} | {'SENTIMENT':<12} | {'PCR OI Δ':<10} | {'ACTION':<10}")
    print("-" * 60)

    # Need mapper loaded
    if bridge.mapper.df.is_empty():
        logger.info("Loading instruments mapper...")
        # Mapper autoloads in __init__ usually, so it should be fine.
        pass

    for symbol in target_names:
        try:
            # 1. Resolve Instrument
            scrip = None
            seg = None
            
            # Index Check
            res = bridge.mapper.df.filter(
                (import_pl_col('SEM_TRADING_SYMBOL') == symbol) & 
                (import_pl_col('SEM_INSTRUMENT_NAME') == 'INDEX')
            )
            if not res.is_empty():
                row = res.row(0, named=True)
                scrip = int(row['SEM_SMST_SECURITY_ID'])
                seg = 'IDX_I'
            else:
                # Stock Check
                res = bridge.mapper.df.filter(
                    (import_pl_col('SEM_TRADING_SYMBOL') == symbol) &
                    (import_pl_col('SEM_EXM_EXCH_ID').is_in(['NSE', 'NSE_EQ', 'EQ']))
                )
                if not res.is_empty():
                    row = res.row(0, named=True)
                    scrip = int(row['SEM_SMST_SECURITY_ID'])
                    seg = 'NSE_EQ' if row['SEM_EXM_EXCH_ID'] == 'NSE' else row['SEM_EXM_EXCH_ID']

            if not scrip:
                logger.warning(f"Could not resolve {symbol}")
                continue

            # 2. Fetch Expiry
            expiries = bridge.fetch_expiry_list(scrip, seg)
            if not expiries:
                continue
            
            current_date = datetime.now().strftime('%Y-%m-%d')
            valid_expiries = [d for d in expiries if d >= current_date]
            if not valid_expiries:
                continue
            
            expiry = sorted(valid_expiries)[0]

            # 3. Fetch Chain
            print(f"Checking {symbol} ({expiry})...", end='\r')
            data = bridge.fetch_option_chain(scrip, seg, expiry)
            
            # RATE LIMIT: 1 request every 3 seconds
            await asyncio.sleep(3.1)
            
            if not data or 'oc' not in data:
                continue
                
            oc = data['oc']
            
            # 4. Calculate Sentiment (Put Delta - Call Delta) across WHOLE chain
            total_ce_change = 0
            total_pe_change = 0
            
            for strike_str, s_data in oc.items():
                ce = s_data.get('ce', {})
                pe = s_data.get('pe', {})
                
                total_ce_change += (ce.get('oi', 0) - ce.get('previous_oi', 0))
                total_pe_change += (pe.get('oi', 0) - pe.get('previous_oi', 0))

            sentiment = total_pe_change - total_ce_change
            
            # Action Signal
            action = "NEUTRAL"
            if sentiment > 500000: action = "BULLISH"
            elif sentiment < -500000: action = "BEARISH"
            
            # Store Result (Use abs sentiment for ranking strength)
            results.append({
                'symbol': symbol,
                'sentiment': sentiment,
                'ce_delta': total_ce_change,
                'pe_delta': total_pe_change,
                'action': action,
                'abs_sent': abs(sentiment)
            })

            # Clear line
            print(" " * 50, end='\r')
            print(f"{symbol:<15} | {sentiment:<12,} | {total_pe_change:<10,} | {action:<10}")

        except Exception as e:
            logger.error(f"Error scanning {symbol}: {e}")

    # Sort by Absolute Sentiment (Strength)
    print("\n" + "=" * 60)
    print("🏆 TOP 10 HIGHEST CONVICTION TRADES (GLOBAL SENTIMENT)")
    print("=" * 60)
    sorted_results = sorted(results, key=lambda x: x['abs_sent'], reverse=True)
    
    for i, res in enumerate(sorted_results[:10]):
        direction = "🟢 BIG BULL (BUY CE)" if res['sentiment'] > 0 else "🔴 BIG BEAR (BUY PE)"
        print(f"{i+1:<2}. {res['symbol']:<12} -> {direction:<20} (Score: {res['sentiment']:+,})")

def import_pl_col(name):
    import polars as pl
    return pl.col(name)

if __name__ == '__main__':
    asyncio.run(run_scan())
