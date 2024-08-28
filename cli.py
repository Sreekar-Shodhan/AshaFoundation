import extraction
import sys
if __name__ == '__main__':
   command = sys.argv[1]
   if command == 'extract_yc':
       extraction.cumulative_funding_yearCurr()
   elif command == 'extract_py':
       extraction.calculate_funding_pidYear()
   elif command == 'only_states':
       extraction.only_states()
   elif command == 'convert_to_DF':
        extraction.convert_to_DF()
   elif command == 'final_df':
        extraction.final_df()