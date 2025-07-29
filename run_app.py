import time
import streamlit as st
import os
from anonymize import modify_dicom_files, erase_and_save_details

st.title("DICOM Anonymization")

folder_a = st.text_input("Enter Folder A path (input DICOM folder)")
folder_b = st.text_input("Enter Folder B path (output base folder)")

if st.button("Go"):
    if not folder_a or not folder_b:
        st.error("Please enter both Folder A and Folder B paths.")
    elif not os.path.isdir(folder_a):
        st.error(f"Folder A path does not exist: {folder_a}")
    elif not os.path.isdir(folder_b):
        st.error(f"Folder B path does not exist: {folder_b}")
    else:
        # Prepare paths based on user inputs
        input_folder = folder_a
        error_folder = os.path.join(folder_b, "err")
        no_report_path = os.path.join(folder_b, "err")
        excel_path = folder_b

        folder_name = os.path.basename(os.path.normpath(folder_a))
        dic_excel_path = os.path.join(folder_b, f"{folder_name}_Institution_names.xlsx")
        extracted_data_excel_path = os.path.join(folder_b, f"{folder_name}_extracted_data.xlsx")
        
    # patient_folders = [f for f in os.listdir(input_folder) if os.path.isdir(os.path.join(input_folder, f))]
    # total_patients = len(patient_folders)

    # st.info(f"🧾 Total Patient Folders: {total_patients}")
        
    patient_folders = [f for f in os.listdir(input_folder) if os.path.isdir(os.path.join(input_folder, f))]
    total_folders = len(patient_folders)
    st.info(f"Total Patient Folders: {total_folders}")

    progress_bar_1 = st.progress(0)
    progress_text_1 = st.empty()

        # Define progress callback function
    def update_progress(count, total):
        progress = count / total
        progress_bar_1.progress(progress)
        progress_text_1 .text(f"Modified {count}/{total} folders")


    try:
            # Call function with progress callback
            dummy_counter, last_folder = modify_dicom_files(input_folder, dic_excel_path, progress_callback=update_progress)
            st.success(" DICOM folders successfully modified.")

            # Proceed with erase_and_save_details (assuming it takes these args)
            st.info("Step 2: Erasing and saving details...")

            progress_bar_2 = st.progress(0)
            progress_text_2 = st.empty()

            def update_erase_progress(count, total):
                progress = count / total
                progress_bar_2.progress(progress)
                progress_text_2.text(f"Progress: {count}/{total} output files/folders ready")
                
            erase_and_save_details(input_folder, excel_path, no_report_path, error_folder, extracted_data_excel_path, progress_callback=update_erase_progress)

            expected_outputs = [
                extracted_data_excel_path,
                dic_excel_path,
                no_report_path,
                error_folder
            ]
            total_outputs = len(expected_outputs)

            while True:
                ready = sum([os.path.exists(p) for p in expected_outputs])
                progress_bar_2.progress(ready / total_outputs)
                progress_text_2.text(f"Progress: {ready}/{total_outputs} output files/folders ready")

                if ready == total_outputs:
                    break
                time.sleep(1)

            st.success("Anonymization and data extraction complete!")

    except Exception as e:
            st.error(f"An error occurred: {e}")
