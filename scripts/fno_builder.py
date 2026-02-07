import polars as pl
import logging
from pathlib import Path


class FNOBuilder:
    def __init__(self):
        self.url = 'https://images.dhan.co/api-data/api-scrip-master-detailed.csv'
        self.output_csv = Path(__file__).parent.parent / 'cache/fno_watchlist.csv'
        self.logger = logging.getLogger(__name__)

    def build(self):
        self.logger.info('Starting Watchlist Build...')

        manual_entries = pl.DataFrame(
            {'SYMBOL': ['NIFTY', 'BANKNIFTY', 'FINNIFTY', 'MIDCPNIFTY'], 'SECURITY_ID': ['13', '25', '27', '442']},
            schema={'SYMBOL': pl.String, 'SECURITY_ID': pl.String},
        )

        try:
            fno_data = (
                pl.scan_csv(self.url)
                .filter(
                    (pl.col('EXCH_ID') == 'NSE')
                    & (pl.col('INSTRUMENT') == 'FUTSTK')
                    & (~pl.col('UNDERLYING_SYMBOL').str.contains('NSETEST'))
                )
                .select(
                    [
                        pl.col('UNDERLYING_SYMBOL').alias('SYMBOL'),
                        pl.col('UNDERLYING_SECURITY_ID').cast(pl.String).alias('SECURITY_ID'),
                    ]
                )
                .collect()
            )

            final_watchlist = pl.concat([manual_entries, fno_data]).unique(subset=['SYMBOL']).sort('SYMBOL')

            final_watchlist.write_csv(self.output_csv)
            self.logger.info(f'Watchlist created with {len(final_watchlist)} unique instruments.')

        except Exception as e:
            self.logger.error(f'Build Failed: {e}', exc_info=True)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
    fno_builder = FNOBuilder()
    fno_builder.build()
