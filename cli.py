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
   elif command == 'state_year':
        extraction.state_year()
   elif command == 'total_year':
        extraction.total_year_df()
   elif command == 'percentage_state':
        extraction.percentage_state_df()
   elif command == 'state_chapter':
        extraction.state_chapter_df()
   elif command == 'state_year_chapter':
        extraction.state_year_chapter()
   elif command == 'percentage_state_year_chapter':
        extraction.percentage_state_year_chapter()
   elif command == 'per_pop_state_year_chapter':
        extraction.per_pop_state_year_chapter()
   elif command == 'per_pop_state_year':
        extraction.per_pop_state_year()
   elif command == 'per_pop_year_state':
        extraction.per_pop_year_state()
   elif command == 'bimaru':
        extraction.bimaru()