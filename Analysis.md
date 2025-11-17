Key anomalies that SHOULD be flagged:
WeekNormal_rateMean (prior 8 weeks)StdDevLimit (Mean + 2×StdDev)Anomaly?2021050.0006410.0003210.0000000.000321YES ✓2021080.0009620.0004490.0001600.000769YES ✓2021090.0009620.0004810.0002290.000939YES (marginal)2021330.0006410.0003210.0000000.000321YES ✓2021400.0012820.0004810.0001600.000801YES ✓2021480.0009620.0006410.0003120.001265NO2022020.0012820.0008010.0002260.001253YES ✓2022040.0016030.0008820.0002740.001430YES ✓2022050.0022440.0010420.0004120.001866YES ✓2022070.0038470.0013630.0005440.002451YES ✓2022200.0028860.0016030.0004420.002487YES ✓2022290.0032060.0017640.0007100.003184YES (marginal)2022300.0025650.0018440.0007480.003340NO2022310.0028860.0020040.0007470.003498NO2022390.0044890.0022440.0006330.003510YES ✓2022510.0041680.0026460.0008280.004302NO
The Real Issue: Your Current Formula Includes Current Week
Looking at your CSV data, I can see the problem:
Week 202229 (the one in your screenshot):

Your TrailingUpperLimit = 0.002966
Normal_rate = 0.003206
Flagged as anomaly ✓ (correct!)

Week 202230:

Your TrailingUpperLimit = 0.003047 (jumped up!)
Normal_rate = 0.002565
NOT flagged ✗ (but should be questionable)

Week 202231:

Your TrailingUpperLimit = 0.003204 (jumped even more!)
Normal_rate = 0.002886
NOT flagged ✗

What's Happening in Your Data
From weeks 202207-202239 (Q1-Q3 2022), you have sustained elevated demand:

Average normal_rate jumps from ~0.0006 to ~0.0025
This is a 4x increase that persists for ~32 weeks

With an 8-week window:

Only the first few spikes get caught (202207, 202220, 202229, 202239)
The rest are missed because the control limits adapt upward to the new elevated level

Weeks That SHOULD Be Flagged (if you want to catch the Q1-Q4 2022 failure):
Using a fixed baseline from 2021 or a longer window, these weeks are clearly anomalous:
2022 Elevated Period (compared to 2021 baseline of ~0.0006):

202202, 202204-202207 (initial spike)
202210-202214 (sustained high)
202215-202225 (continued high)
202228-202235 (peak period)
202237-202252 (continued elevation)

Essentially weeks 202202-202252 (50 weeks!) are elevated compared to 2021 baseline.
My Recommendation
Your 8-week window is too short to detect a sustained 50-week failure pattern. You need one of these approaches:
Option 1: Fixed Baseline (BEST for this data)
Use 2021 Q3-Q4 as your baseline:
daxVAR BaselineStart = DATE(2021, 7, 1)  -- Q3 2021
VAR BaselineEnd = DATE(2021, 12, 31)  -- Q4 2021
This would flag almost all of 2022 as anomalous (which seems correct based on your 4x rate increase).
Option 2: 26-Week Window
This would catch more of the pattern but still eventually adapt.
What caused this sustained 50-week elevation in 2022? That context would help determine the right detection approach.







MEAN 8 WEEKS

excel=IF(D2<=8, "", 
    AVERAGE(
        IF(($A$2:$A2=$A2)*($D$2:$D2>=D2-8)*($D$2:$D2<D2), 
            $C$2:$C2
        )
    )
)


STD

=IF(D2<=8, "", 
    STDEV.S(
        IF(($A$2:$A2=$A2)*($D$2:$D2>=D2-8)*($D$2:$D2<D2), 
            $C$2:$C2
        )
    )
)
UPPER LIMIT
=IF(E2="", "", E2 + (2 * F2))
ANOMALY
=IF(G2="", "", IF(C2>G2, 1, 0))
