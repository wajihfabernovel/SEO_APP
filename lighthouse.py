import streamlit as st
import json

# Function to parse and display a single Lighthouse report
def display_lighthouse_report(report_data):
    # Display the categories with scores and colors
    categories = report_data.get('categories', {})
    for category, details in categories.items():
        score = details.get('score')
        if score is not None:
            score = float(score)  # Ensure the score is a float
            score_percentage = round(score * 100,1)
            score_progress = score
            color = "success" if score_percentage > 89 else "warning" if score_percentage > 49 else "danger"
        else:
            score_percentage = "No score available"
            score_progress = 0
            color = "secondary"

        st.markdown(f"### {category.replace('-', ' ').title()}")
        st.progress(score_progress)
        st.markdown(f"<span class='badge bg-{color}'>{score_percentage if isinstance(score_percentage, float) else score}</span>", unsafe_allow_html=True)

    # Display the audits
    audits = report_data.get('audits', {})
    for audit, details in audits.items():
        score = details.get('score')
        if score is not None:
            score = float(score)  # Ensure the score is a float
            score_percentage = round(score * 100,1)
            score_progress = score
            color = "success" if score_percentage > 89 else "warning" if score_percentage > 49 else "danger"
        else:
            score_percentage = "No score available"
            score_progress = 0
            color = "secondary"
        
        with st.expander(f"{audit.replace('-', ' ').title()}"):
            st.write(details.get('description'))
            st.progress(score_progress)
            st.markdown(f"<span class='badge bg-{color}'>{score_percentage if isinstance(score_percentage, float) else score}</span>", unsafe_allow_html=True)

# Custom styles
def local_css(file_name):
    with open(file_name) as f:
        st.markdown(f'<style>{f.read()}</style>', unsafe_allow_html=True)

# Streamlit app layout
def app():
    st.title('Lighthouse Reports Visualizer')

    # Apply the custom CSS styles
    local_css("style.css")

    # File uploader allows user to add their own report(s)
    uploaded_files = st.file_uploader("Upload your Lighthouse JSON report(s)", type=['json'], accept_multiple_files=True)
    if uploaded_files:
        tabs = st.tabs([f.name for f in uploaded_files])
        for uploaded_file, tab in zip(uploaded_files, tabs):
            with tab:
                report_data = json.load(uploaded_file)
                display_lighthouse_report(report_data)
    else:
        st.warning("Please upload one or more Lighthouse JSON reports to proceed.")

# Run the app
if __name__ == "__main__":
    app()
