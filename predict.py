import joblib
import numpy as np
import streamlit as st

Introduction = st.container()
Dataset = st.container()
Modelling = st.container()

with Introduction:
    st.title(':blue[Client Conversion Estimator] :sunglasses:')
    st.text('This estimator helps predict clients who are likely going to take a conversion action, helping you understand clients to focus resources on. This is resourceful for marketing professionals and business owners.')
with Dataset:
    st.header('Dataset')
    st.text('This dataset provides a comprehensive view of customer interactions with digital marketing campaigns. It includes demographic data, marketing-specific metrics, customer engagement indicators, and historical purchase data, making it suitable for predictive modeling and analytics in the digital marketing domain.')
    st.caption(' Credits to: Rabie El Kharoua. (2024). 📈 Predict Conversion in Digital Marketing Dataset [Data set]. Kaggle. https://doi.org/10.34740/KAGGLE/DSV/8742946 for providing this dataset')
with Modelling:
    st.header('Prediction Machine')
    st.text('This model was trained using Random Forest Classifier and consists of 8000 records of different clients marketing interactions and conversion activities.')
    st.text('Instructions: Just enter the information of the client below and click predict to generate a result. Any unknown variable or information should be recorded as 0. No special characters such as commas or currency symbols.')

    model = joblib.load('marketing_predictor3.joblib')
    def main():
        st.subheader('Client Conversion Predictor')
        Age = st.text_input('What is the age of the client?')
        Income = st.slider('What is the income range of the client?', min_value = 10000, max_value= 1000000, value = 10000, step = 10000)
        Campaign_Channel = st.selectbox('What campaign channel can you attribute this customer to?', options = ['Social Media', 'Email', 'PPC', 'Referral', 'SEO', 'Others'])
        AdSpend = st.text_input('How much did you spend advertising on this channel?')
        CampaignType = st.selectbox('What is the goal of the campaign?', options = ['Awareness', 'Retention', 'Conversion', 'Consideration'])
        ClickThroughRate = st.text_input('What is the click through rate of the campaign?')
        ConversionRate = st.text_input('What was the conversion rate for this type of campaign that you ran in the past?')
        WebsiteVisits = st.slider('Do you know how many times this client visited your website?', min_value = 0, max_value= 15, value = 0, step = 1)
        PagesPerVisit = st.slider('Do you know how many pages on your website this client visted?', min_value = 0, max_value= 15, value = 0, step = 1)
        TimeOnSite = st.slider('How many minutes did this client spend on your website?', min_value = 0, max_value= 15, value = 0, step = 1)
        SocialShares = st.text_input('How many times did this client share your content')
        EmailOpens = st.text_input ('How many emails did the client open?')
        EmailClicks = st.text_input('How many email clicks action can you estimate the client did?')
        PreviousPurchase = st.text_input('How many purchases has this client made in the past?')
        if st.button('Predict'):
            makeprediction = model.predict([[Age, Income, AdSpend,ClickThroughRate, ConversionRate, WebsiteVisits, PagesPerVisit,TimeOnSite, SocialShares, EmailOpens, EmailClicks,PreviousPurchase]])
            output = makeprediction
            st.success('Marketing genie computes {}'.format(output))
            if output == 0:
                st.success('This client has a low probability of converting')
            else:
               st.success('This client has a high probability of converting')
    if __name__ == '__main__':
        main()
