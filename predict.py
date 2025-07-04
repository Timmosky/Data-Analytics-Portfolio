import joblib
import numpy as np
import streamlit as st

Introduction = st.container()
Dataset = st.container()
Modelling = st.container()

with Introduction:
    st.title(':blue[Client Conversion Estimator] :sunglasses:')
    st.text('This estimator helps predict prospects who are likely going to take a conversion action, helping you understand prospects to focus resources on. This is resourceful for marketing professionals and business owners.')
with Dataset:
    st.header('Dataset')
    st.text('This dataset provides a comprehensive view of customer interactions with digital marketing campaigns. It includes demographic data, marketing-specific metrics, customer engagement indicators, and historical purchase data, making it suitable for predictive modeling and analytics in the digital marketing domain.')
    st.caption(' Credits to: Rabie El Kharoua. (2024). 📈 Predict Conversion in Digital Marketing Dataset [Data set]. Kaggle. https://doi.org/10.34740/KAGGLE/DSV/8742946 for providing this dataset')
with Modelling:
    st.header('Prediction Machine')
    st.text('This model was trained using Random Forest Classifier and consists of 8000 records of different clients marketing interactions and conversion activities.')
    st.text('📢 Read Me : Enter the information of the prospect below and click predict to generate a result. Any unknown information should be recorded as 0 else model may generate an error. No special characters such as commas or currency symbols. Assume currency to be USD $')

    model = joblib.load('marketing_predictor3.joblib')
    def main():
        st.subheader('Prospect Conversion Predictor')
        Age = st.text_input('What is the age of the prospect?')
        Income = st.slider('What is the monthly income($) of the prospect?', min_value = 1000, max_value= 300000, value = 1000, step = 1000)
        Campaign_Channel = st.selectbox('What campaign channel can you attribute this prospect to?', options = ['Social Media', 'Email', 'PPC', 'Referral', 'SEO', 'Others'])
        AdSpend = st.text_input('How much did you spend advertising on this channel?')
        CampaignType = st.selectbox('What is the goal of the campaign?', options = ['Awareness', 'Retention', 'Conversion', 'Consideration'])
        ClickThroughRate = st.text_input('What is the click through rate of the campaign?')
        ConversionRate = st.text_input('What was the conversion rate for this type of campaign that you ran in the past?')
        WebsiteVisits = st.slider('Do you know how many times this prospect visited your website?', min_value = 0, max_value= 15, value = 0, step = 1)
        PagesPerVisit = st.slider('Do you know how many pages on your website this prospect visted?', min_value = 0, max_value= 15, value = 0, step = 1)
        TimeOnSite = st.slider('How many minutes did this prospect spend on your website?', min_value = 0, max_value= 15, value = 0, step = 1)
        SocialShares = st.text_input('How many times did this prospect share your content')
        EmailOpens = st.text_input ('How many emails did this prospect open?')
        EmailClicks = st.text_input('How many email clicks action can you estimate the prospect made?')
        PreviousPurchase = st.text_input('How many purchases has this prospect made in the past?')
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
