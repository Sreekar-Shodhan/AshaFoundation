import streamlit as st
import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

st.title("Asha Foundation Funding Analysis")

# Getting the working directory of main.py
working_dir = os.path.dirname(os.path.abspath(__file__))
folder_path = os.path.join(working_dir, 'DataCSV')

# List the files in datafolder
files = [f for f in os.listdir(folder_path) if f.endswith('.csv')]

# Dropdown for all files
selected_file = st.selectbox('Select a File', [''] + files)

if selected_file:
    # Get the complete path of selected file
    file_path = os.path.join(folder_path, selected_file)
    
    # Read the csv file as pd df
    df = pd.read_csv(file_path)
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.write('Data Preview:')
        st.write(df.head())
    
    with col2:
        # User selection of df columns
        columns = df.columns.tolist()
        x_axis = st.selectbox('Select the x-axis', options=[''] + columns)
        y_axis = st.selectbox('Select the y-axis', options=[''] + columns)
        
        plot_list = ['Line Plot', 'Bar Chart', 'Scatter Plot', 'Distribution Plot', 'Count Plot']
        selected_plot = st.selectbox('Select a plot', options=[''] + plot_list)
    
    # Button to generate plot
    if st.button('Generate Plot'):
        if x_axis and (y_axis or selected_plot in ['Distribution Plot', 'Count Plot']):
            fig, ax = plt.subplots(figsize=(10, 6))
            
            try:
                if selected_plot == 'Line Plot':
                    sns.lineplot(data=df, x=x_axis, y=y_axis, ax=ax)
                elif selected_plot == 'Bar Chart':
                    sns.barplot(data=df, x=x_axis, y=y_axis, ax=ax)
                elif selected_plot == 'Scatter Plot':
                    sns.scatterplot(data=df, x=x_axis, y=y_axis, ax=ax)
                elif selected_plot == 'Distribution Plot':
                    sns.histplot(data=df, x=x_axis, kde=True, ax=ax)
                    y_axis = 'Density'
                elif selected_plot == 'Count Plot':
                    sns.countplot(data=df, x=x_axis, ax=ax)
                    y_axis = 'Count'
                
                # Adjust label sizes
                ax.tick_params(axis='x', labelsize=10, rotation=45)
                ax.tick_params(axis='y', labelsize=10)
                
                # Title and axes labels
                plt.title(f'{selected_plot} of {y_axis} vs {x_axis}', fontsize=14)
                plt.xlabel(x_axis, fontsize=12)
                plt.ylabel(y_axis, fontsize=12)
                
                # Adjust layout
                plt.tight_layout()
                
                st.pyplot(fig)
            except Exception as e:
                st.error(f"An error occurred: {str(e)}")
        else:
            st.warning("Please select both x and y axes (except for Distribution and Count plots).")
else:
    st.info("Please select a file to begin.")