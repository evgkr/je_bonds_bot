# Je Bonds Bot User Manual
Version: 10 Jan 2026
About this manual
This document explains what the bot does, how its bond metrics are computed, and how to use
each Telegram menu efficiently. Button names and icons are written exactly as you see them in
the chat, so you can follow the steps without guessing.
Important: the bot provides analytics, not investment advice. Calculations are based on
available market data and a set of simplifying assumptions; they do not include taxes, broker
fees, or execution slippage. Always verify critical numbers in your broker terminal before trading.
What you will find here
- 
How and when data updates (operating hours, real-time feeds, hourly rating refresh).
- 
Definitions and formulas for every key metric (YTM/YTC/YTW, durations, DPP, DtA, liquidity
fields, and weighted “(w)” versions).
- 
Step-by-step usage of the Screener (filtering, sorting, grouping, fields, settings, presets).
- 
Step-by-step usage of Alerts (building rules, expression logic, and notification modes).
- 
How to use Watchlist and Blacklist as helper tools (favorites vs exclusions).
How to use this manual
- 
For fastest results, use it like a reference: open the section you need, follow the buttons
exactly as shown, and come back when you change settings or want to verify a metric.
- 
Quick start: read “General information” once, then jump to “Screener function” for daily work.
- 
Automation: after Screener, read “Alerts function” to set up notifications and digests.
- 
Troubleshooting: if a number looks unusual, return to “General information” where
assumptions, units, and formulas are listed.

---

## General information
How the instrument works
The Bot is active 24/7 for an end user, though the operating hours of the engine itself are
only 18 hours a day. The engine is active daily from 06:06 to 00:06 (MSK, UTC+3). During this
window, prices/volumes are streamed and metrics are recalculated as soon as new data
arrives. From 00:06 to 06:06 the engine pauses market streams (a quiet window), so metrics
are not updated until the next start. Again – the telegram part is still active, it is only bonds data
which is stopped since the market is closed.
Real-time data sources. Market data (daily candle close and volume) is received through a
broker market-data API. Reference bond parameters (nominal, accrued interest, maturity,
coupons schedule, issue size, sector, country) are also pulled via API. Credit ratings and
offer/call dates are obtained from Smart-Lab.
Refresh cycles. Prices/volumes update continuously during operating hours. Credit ratings and
offer/call dates are refreshed every hour (outside the quiet window).
Instrument coverage and units
- 
Universe / coverage. The screener covers RUB-denominated bonds that match the
following conditions: fixed coupon (floating-coupon bonds are excluded), no amortization,
not perpetual. Instruments without a credit rating on Smart-Lab are excluded from the
universe.
- 
Price field. Price is shown as an estimated money price per 1 bond in RUB: Price =
(close_quote / 100) * Nominal. This is a practical approximation derived from the daily
candle close quote.
- 
Accrued interest (ACI). Accrued Interest is taken from the API and is in RUB per 1 bond.
- 
Yields. YTM/YTC/YTW are annualized (per year) and displayed in %. Internally they are
stored as decimals (e.g., 0.125 = 12.5%).
- 
Durations. Macaulay/Modified durations are shown in years.
- 
DtA (days-to-event). DtM/DtC/DtW are calendar days.
- 
Volume. Vol is daily trading volume in lots or in rubles from the candle stream.
- 
Issue size. Issue Size is the number of bonds in the issue (as provided by the API). Issue
(RUB) equals IssueSize * Nominal.
Cashflow conventions and settlement rule
All cashflow-based metrics are computed on actual coupon dates (no month-end
`approximations). Time to each cashflow is measured on an ACT/365 basis:`
`t_i = (Date_i - Today) / 365`
Dirty price (initial investment / PV) is computed in RUB per 1 bond:
PV = Price + AccruedInterest
Settlement adjustment (important). When you buy today, settlement is T+1 (adjusted for nontrading days). If the first upcoming coupon’s fix date is earlier than the settlement date, that
coupon is excluded from the cashflows (because the buyer is not entitled to it).

---

## Yield metrics (YTM, YTC, YTW)
All yields are computed as XIRR on actual dates. In simplified discounted-cashflow form, YTM is
the annual rate y that solves:
`0 = -PV + SUM_{i=1..N} [ CF_i / (1 + y)^(t_i) ] + Nominal / (1 + y)^(t_M)`
Yield to Call/Offer (YTC) is computed in the same way, but only coupons up to the offer/call date
are included, and the nominal redemption is assumed to occur on the offer/call date at nominal
(par):
`0 = -PV + SUM_{i=1..K} [ CF_i / (1 + y)^(t_i) ] + Nominal / (1 + y)^(t_C)`
Yield to Worst (YTW) selects the lower of YTM and YTC if an offer/call date exists; otherwise
YTW equals YTM:
If offer/call exists: YTW = min(YTM, YTC); else: YTW = YTM
Practical notes: yields do not include broker commissions, taxes, FX conversion, or execution
slippage. Future cashflows are taken from the provided coupon schedule; bonds with floating
coupons are excluded from the universe.
## DtA metrics (DtM, DtC, DtW)
Days-to-event metrics are calendar days:
DtM = (MaturityDate - Today).days
DtC = (OfferDate - Today).days  (if no offer/call: DtC = 0)
DtW (days to worst) is chosen based on which yield is worse. For non-weighted values it follows
YTM vs YTC; for weighted values it follows YTM(w) vs YTC(w):
If YTC < YTM then DtW = DtC, else DtW = DtM. If YTC(w) < YTM(w) then DtW(w) = DtC, else
DtW(w) = DtM.
## Duration and convexity (Macaulay, Modified)
For a chosen yield y (YTM, YTC, or YTW) and cashflows CF_i at times t_i:
`PV(CF_i) = CF_i / (1 + y)^(t_i)`
`MacaulayDuration = [ SUM_i ( t_i * PV(CF_i) ) ] / PV`
`ModifiedDuration = MacaulayDuration / (1 + y)`
`Convexity = [ SUM_i ( t_i * (t_i + 1) * CF_i / (1 + y)^(t_i + 2) ) ] / PV`
## DPP (price sensitivity to a 1 p.p. yield move)
DPP estimates the relative price change (DeltaP / P) using the duration-convexity approximation
`with DeltaY = 0.01:`
`DPP_down_1pp  = +ModifiedDuration * DeltaY + 0.5 * Convexity * (DeltaY^2)`
`DPP_up_1pp    = -ModifiedDuration * DeltaY + 0.5 * Convexity * (DeltaY^2)`
## Weighted “(w)” metrics (credit-risk weighting)

---

Fields with the “(w)” suffix apply a simplified credit-risk adjustment using the issuer’s Smart-Lab
credit rating. The idea: future cashflows are multiplied by an estimated survival probability S(t).
This produces ‘survival-weighted’ cashflows and, from them, weighted yields and weighted risk
metrics.
### Step 1 — default probabilities by rating and hazard rates.
For each rating bucket we use cumulative default probabilities PD_1Y, PD_2Y, PD_3Y and
convert them into piecewise hazard rates:
S1 = 1 - PD_1Y;  S2 = 1 - PD_2Y;  S3 = 1 - PD_3Y
`lambda1 = -ln(S1)`
`lambda2 = ln(S1 / S2)`
`lambda3 = ln(S2 / S3)`
### Step 2 — survival probability to any date.
t = (TargetDate - Today).days / 365
S(t) is then computed piecewise:
`if t <= 1:   S(t) = exp(-lambda1 * t)`
`if 1 < t <= 2: S(t) = exp(-lambda1 - lambda2 * (t - 1))`
`if t > 2:    S(t) = exp(-lambda1 - lambda2 - lambda3 * (t - 2))`
### Step 3 — build survival-weighted cashflows.
Coupon_i(w)  = Coupon_i * S(t_i)
Nominal(w)   = Nominal * S(t_redemption)
The initial purchase cashflow (-PV) is not weighted.
### Step 4 — compute “(w)” metrics.
YTM(w), YTC(w), and YTW(w) are computed as XIRR using the weighted cashflows. Weighted
DtW(w) is chosen by comparing YTC(w) vs YTM(w) (the smaller yield determines the ‘worst’
date).
Duration(w), Convexity(w), and DPP(w) are computed on the survival-weighted cashflows but
using the corresponding non-weighted yield (e.g., Duration_YTM(w) uses weighted cashflows
with y = YTM; Duration_YTC(w) uses weighted cashflows with y = YTC). This keeps sensitivity
measures comparable to the base yield used in discounting.
Ratings update every hour, so all “(w)” metrics can change after a rating refresh even if the
market price stays the same.
## Liquidity metrics (Volume, Volume/Issue size)
VolMoney is an approximate traded value proxy based on the latest price:
VolMoney = Vol * Price
Volume / Issue size (%) is computed as:

---

VolIssuePct = (Vol * 100) / IssueSize
## Assumptions and limitations
- 
Near real-time does not mean tick-perfect: upstream API latency, network issues, and
Telegram rate limits can cause delays.
- 
All calculations exclude broker commissions, taxes, and any execution slippage. Use the
screener for analytics, not as a trade confirmation – in other words you are the only one
responsible for the actions performed, bot does not provide any calls to actions.
- 
Offer/call modelling assumes redemption at nominal (par) on the offer/call date. If the real
terms differ (premium/partial/complex), results may deviate.
- 
The “(w)” model is a simplified survival-weighting approach. It is not a full structural credit
model and does not model recovery rates or spread term structure.
- 
If a bond temporarily has no valid last price, it can be removed from the active calculation
universe until prices become available again.

---

## Screener function (/screener, /sc)
### 1) What “Screener” is and what it does
Screener is a real-time, interactive bond shortlist: it pulls the latest available data, instantly
recalculates results when you change settings, and lets you explore the universe of bonds
without leaving the chat. In a few taps you can scan, filter, sort, and group instruments to quickly
find candidates that match your yield, risk, liquidity, and maturity preferences. See a list of
bonds with a configurable set of columns (“Fields”).
- 
Sort the list by different metrics (yield, duration, etc.) and choose ascending/descending
order.
- 
Group the list (e.g., by sector or rating) and show the “top N” bonds inside each group.
- 
Filter bonds by numeric ranges (e.g., YTW between 10–15) and by categorical values (e.g.,
ratings, sectors).
- 
Optionally view Watchlist-only results and/or exclude bonds via a Blacklist.
The top of the screener message always includes:
- 
Current Sort, Order, Group, Group size, and a detailed Filters summary.
Default behavior on first launch
- 
Default fields: Name, YTW, Price.
- 
Default rating filter: only AAA, AA, A, BBB. (Bonds without a rating will not pass that filter.)
- 
Default sort: YTW, descending (the sorting parameter is always shown in Bold).
- 
Default grouping: Sector.

### 2) How to open the Screener
Users start it via the bot commands:
- 
/screener (or /sc) to open the screener message and interactive keyboard.
Important UI note
- 
Screener is a single message that updates (edits) as the user presses buttons.
- 
If the user runs /screener again, the bot deactivates/removes the keyboard on the previous
screener message and creates a new instance.

---

### 3) Understanding the Screener screen layout
#### 3.1 Header (top of the message)
Header shows:
- 
Title: “  Bond screener”
- 
Sort: Sort: | Order: Asc/Desc
- 
Group: Group: | Group size:

**Images from page 7:**
![Page 7](img/p07_01.jpg)

---

- 
Filters: a multi-line summary of what’s currently active (yields, duration, price, ratings,
sectors, countries, coupons, offer mode, etc.).
- 
Fields: shows the currently active fields displayed

#### 3.2 Rows (bond results)
Each bond is displayed as one line with selected fields. Typically the Name (or Ticker,
depending on field order) is a clickable link to the bond page (Tinkoff link).
#### 3.3 Pagination footer
At the very bottom of the text content, the screener appends:
- 
— Page X/Y —
#### 3.4 Navigation row (page controls)
Below the buttons, the screener shows navigation controls:
- 
⏮ first page
- 
◀️ previous page
- 
X/Y (not clickable; informational)
- 
▶️ next page
- 
⏮ last page

### 4) Main menu (what each main button does)
When the screener is in the main menu, the keyboard shows these buttons:
- 
Sort
- 
Group
- 
Filter

**Images from page 8:**
![Page 8](img/p08_01.png)
![Page 8](img/p08_02.png)

---

- 
Fields
- 
Settings
- 
Presets
The bot can also return you to the main menu automatically:
- 
If you stay inside a submenu and don’t press anything for 30 seconds, it returns to the main
menu keyboard.

### 5) Sort menu (Sort button)
#### 5.1 Sort menu overview
Press Sort to open the sorting menu. Buttons include:
- 
Yield ▸
- 
Duration ▸
- 
% change if yield ↓ 1% ▸
- 
DtA ▸
- 
Last price
- 
Volume
- 
Issue size
- 
Volume/Issue size
- 
← Back
What happens when you choose a sort
- 
The chosen metric becomes the active sort field.
- 
The list rerenders immediately with the new ordering.
- 
The screener ensures the active sort field is included in visible fields in bold (so users can
see what they’re sorting by).

#### 5.2 Sort ▸ Yield
Inside Yield ▸, users can sort by:
- 
YTM, YTC, YTW
- 
Weighted versions: YTM (w), YTC (w), YTW (w)
What “(w)” means in UI

**Images from page 9:**
![Page 9](img/p09_01.jpg)

---

- 
In this bot, “(w)” is treated as a separate precomputed metric (weighted variant) and can be
sorted/filtered independently.

#### 5.3 Sort ▸ Duration
Inside Duration ▸, users can sort by:
- 
Macaulay duration for YTM / YTC / YTW
- 
Modified duration for YTM / YTC / YTW
- 
And weighted versions (w) for each variant

#### 5.4 Sort ▸ % change if yield ↓ 1%
Inside % change if yield ↓ 1% ▸, users can sort by:
- 
% change if yield ↓ 1% — YTM and (w)
- 
% change if yield ↓ 1% — YTC and (w)
- 
% change if yield ↓ 1% — YTW and (w)
These are the “DPP” scenario metrics (price change sensitivity scenario).

**Images from page 10:**
![Page 10](img/p10_01.jpg)
![Page 10](img/p10_02.jpg)

---

#### 5.5 Sort ▸ DtA
Inside DtA ▸, users can sort by:
- 
DtM (days to maturity)
- 
DtC (days to next call/offer)
- 
DtW (days to “worst”/effective term)

#### 5.6 Sort single-click items
The following items in Sort menu apply immediately without a submenu:
- 
Price
- 
Volume
- 
Issue size
- 
Volume/Issue size
- 
Reset sort (returns to default sort settings)

### 6) Group menu (Group button)
Press Group to open grouping options. Choices:
- 
No group
- 
Credit rating
- 
Sector
- 
Country
- 
Coupons per year

**Images from page 11:**
![Page 11](img/p11_01.jpg)
![Page 11](img/p11_02.jpg)

---

- 
Put/Call option
- 
Time to maturity (buckets)
- 
Issue size (buckets)
- 
← Back

The chosen type of grouping is ticked.
#### 6.1 What grouping does
- 
The screener splits results into groups (e.g., each sector).
- 
For each group, it shows a header with group name and count, then shows only the top N
items inside that group, where N = Group size.
#### 6.2 Group size (how many items per group)
- 
“Group size” is configured in Settings (see section 9).
- 
Example: if Group size = 7, the screener shows up to 7 bonds per group.
#### 6.3 How to remove grouping
- 
Select No group.

**Images from page 12:**
![Page 12](img/p12_01.jpg)

---

### 7) Filter menu (Filter button)
Press Filter to open the filter menu. It contains:
- 
Submenus: Yield ▸, Duration ▸, % change if yield ↓ 1% ▸, DtA ▸
- 
Range filters: Price, Volume (₽), Par value, Issue size
- 
Categorical filters: Credit ratings, Sectors, Countries, Coupons per year

**Images from page 13:**
![Page 13](img/p13_01.png)
![Page 13](img/p13_02.png)
![Page 13](img/p13_03.png)
![Page 13](img/p13_04.png)

---

- 
Mode filter: Put/Call option (toggle cycles)
- 
Clear filters
- 
← Back

### 8) How range filters work (the most important part)
Whenever you tap a numeric filter (like Price or YTW), the bot will ask you to type a range
condition.
#### 8.1 Accepted input formats
Range parsing supports:
- 
min-max (inclusive)
- 
>=x, <=y
- 
>x, <y
- 
Decimal separator: . or , is accepted (comma is converted to dot).
#### 8.2 Percent sign behavior (critical)
If the user types a trailing %, the bot interprets the numbers as percent and divides by 100
internally. Example:
- 
Input 10-15% becomes 0.10–0.15 internally.
Important note

**Images from page 14:**
![Page 14](img/p14_01.jpg)

---

- 
For yield filters in this screener, users should typically enter numbers without % (e.g., 1015) because yields are displayed in percent in the UI.
- 
Only include % if your data model truly expects fractional values for that particular metric.
#### 8.3 What happens if the input is invalid
If the bot can’t parse the range, it replies with an error message and examples like:
- 
10-15, >=12.3, <=9.5, >12.3, <9.5
Or with % variants for percent units.
#### 8.4 After a valid input
- 
The filter is stored.
- 
The screener rerenders immediately.
- 
The bot deletes the prompt and the user’s numeric message (to keep chat clean).

### 9) Filter submenus (Yield / Duration / % change / DtA)
#### 9.1 Filter ▸ Yield
Buttons:
- 
YTM, YTC, YTW, and weighted versions (w)
- 
← Back

#### 9.2 Filter ▸ Duration
Buttons include:
- 
Macaulay and Modified duration variants for YTM/YTC/YTW, plus weighted variants
- 
← Back

**Images from page 15:**
![Page 15](img/p15_01.jpg)
![Page 15](img/p15_02.png)
![Page 15](img/p15_03.jpg)

---

#### 9.3 Filter ▸ % change if yield ↓ 1%
Buttons include:
- 
% dP@-1% — YTM, YTC, YTW and weighted variants
- 
← Back

#### 9.4 Filter ▸ DtA
Buttons include:
- 
DtM, DtC, DtW
- 
← Back

### 10) Categorical filters (Ratings / Sectors / Countries / Coupons)
#### 10.1 Credit ratings
Press Credit ratings to open a picker where each rating can be toggled on/off. Selected items
show ✓.
Buttons:
- 
Rating buttons (AAA, AA, … down to D) in a fixed order.
- 
Готово (“Done”) returns to Filter menu.
Default
- 
By default, ratings filter is set to AAA, AA, A, BBB.

#### 10.2 Sectors

**Images from page 16:**
![Page 16](img/p16_01.jpg)
![Page 16](img/p16_02.jpg)

---

Press Sectors to open a sector list:
- 
Each sector can be toggled; selected sectors show ✓.
- 
Unknown/empty sector is displayed as Other.
- 
Готово returns to Filter menu.

#### 10.3 Countries
Press Countries to open the country list:
- 
Empty/None country is shown as Unknown.
- 
Toggle selection with ✓.
- 
Готово returns to Filter menu.

#### 10.4 Coupons per year
Press Coupons per year to open a picker with values:
- 
0, 1, 2, 4, 6, 12, 24 (each toggles, ✓ indicates selected)
- 
Готово returns to Filter menu.

### 11) Put/Call option filter (cycle button)
In the Filter main menu, Put/Call option cycles through modes:
- 
including: show bonds both with and without an offer date (no restriction)
- 
excluding: show only bonds without put/call offer
- 
only: show only bonds with put/call offer

### 12) Clear filters

**Images from page 17:**
![Page 17](img/p17_01.jpg)
![Page 17](img/p17_02.jpg)

---

Press Clear filters to reset the filter state to an empty FilterState (i.e., it clears everything).

### 14) Settings menu (Settings button)
Settings buttons include:
- 
Order: Desc/Asc (toggle)
- 
Group size: N (set numeric)
- 
Fields (display) (opens Fields)
- 
Watchlist / All list (toggle)
- 
← Back
#### 14.1 Order
Toggles sort order:
- 
Desc ↔ Asc
#### 14.2 Group size
Prompts user:
- 
“Введите новое значение Group size (>=1):”
User types an integer. This controls how many items show per group.
#### 14.3 Fields (display)
Fields control which columns are shown in each bond line.
Fields root screen
The Fields menu is structured by categories:
- 
Yield
- 
Duration
- 
Dpp
- 
DtA
- 
Others
- 
Back

Inside a category (toggling fields)
In each category screen:
- 
Tap a field to toggle it on/off.
- 
Selected fields show a ✓.

**Images from page 18:**
![Page 18](img/p18_01.jpg)

---

- 
Press ← Back to return to Fields root.

Safety rules
- 
If the user removes both Name and Ticker, the bot automatically adds Ticker back so that
each row still has an identifier.
- 
The bot also ensures the current sort field is always included in displayed fields.
#### 14.4 Watchlist / All list
Toggles watchlist mode:
- 
In Watchlist mode, screener shows only tickers in the user watchlist (if the watchlist is
empty, the filter effectively doesn’t limit results).
#### 14.5 Bond to blacklist
Prompts user:
- 
“Введите тикеры для чёрного списка через пробел/запятую.”
User enters tickers separated by spaces or commas, which get added to blacklist.

### 15) Presets menu (Presets button)
#### 15.1 What presets do
A preset applies a full configuration:
- 
Fields
- 
Sort field
- 
Whether sort is weighted (w)
- 
Order (asc/desc)
- 
Group mode

**Images from page 19:**
![Page 19](img/p19_01.jpg)
![Page 19](img/p19_02.jpg)

---

#### 15.2 Built-in presets (what each contains)
Built-in presets include:
- 
Default
- 
Desktop 1/2/3
- 
Phone 1/2/3

Their exact configurations are:
- 
Default: fields [Name, YTW, Price], sort YTW, desc, group sector
- 
Desktop 1: [Name, YTW, Price, Rating, DtW, D_mac@YTW, Vol₽, Vol/Issue%], group
sector
- 
Desktop 2: [Name, YTW, YTW (w), Price, Vol₽, Issue (₽), Vol/Issue%, DtW], group sector
- 
Desktop 3: [Name, YTW, Sector, D_mac@YTW, % change if yield ↓ 1% @YTW, Vol₽,
Coupons/year], group rating
- 
Phone 1: [Name, YTW, Price, Rating], group sector
- 
Phone 2: [Name, YTW, Price, DtW], group sector
- 
Phone 3: [Name, YTW, Price, Vol₽], group rating
#### 15.3 Saving user presets
The presets menu also includes:
- 
Save current — saves the current configuration as a user preset (the bot will ask for a
name).
- 
User-saved presets appear as additional buttons.
- 
← Back returns to main.

**Images from page 20:**
![Page 20](img/p20_01.jpg)
![Page 20](img/p20_02.jpg)

---

### 16) Quick “How do I…?” recipes
“I want only bonds with YTW between 12 and 16”
1. Open Filter.
2. Tap Yield ▸.
3. Tap YTW.
4. Reply with 12-16. (No % sign.)
“I want only bonds with put/call offer”
1. Open Filter.
2. Tap Put/Call until it shows only.
“I want to see fewer items per group”
1. Open Settings.
2. Tap Group size: N.
3. Enter a smaller integer (e.g., 5).

**Images from page 21:**
![Page 21](img/p21_01.png)
![Page 21](img/p21_02.jpg)

---

## Alerts function (/alerts, /alert, /al)
### 1) What Alerts Do
Alerts are a rule-based monitoring and notification system for bonds. Instead of you repeatedly
checking the market manually, an alert acts like an automated watcher: it continuously (or on a
defined schedule) evaluates the latest bond data against a set of criteria you define, and notifies
you when a bond enters the “match” state—meaning the bond now satisfies your rule.
An alert is made of:
1. Scope — which bonds the alert should track (whole market, your watchlist, or specific
bonds).
2. Filters — narrow the universe (sector, country, issue size, number of coupons per year, par
value, Put/Call, or by removing specific bonds)
3. Conditions — numeric or categorical checks (e.g., “Yield > 12%”, “Duration < 2y”, “Rating is
BBB or higher” etc.). Conditions are combined via a logical expression (AND/OR +
parentheses).
4. Trigger mode — how you want to be notified:
o
Once (on crossing into “true”)
o
Perpetually
o
Daily digest
o
Weekly digest
In short: Alerts turn your bond criteria into an automated detector. When the market
changes (in real time) and a bond crosses into your defined criteria, you don’t have to discover
it—the bot pushes it to you, in the format and frequency you chose (immediate alerts or
scheduled digests).

### 2) How to Open Alerts
Commands
You can open the Alerts UI using any of these commands in the bot chat:
- 
/alerts
- 
/alert
- 
/al
After running the command, the bot shows the Alerts Home screen.

**Images from page 22:**
![Page 22](img/p22_01.jpg)

---

### 3) Alerts Home Screen (Root)
The home screen is the landing page where you see how many alerts you have and how to
proceed.
#### 3.1 What You See
The message contains:
- 
Title: “  Alerts”
- 
Count of active (enabled) alerts: “Active: N”
- 
List of up to 20 active alerts, numbered with emoji digits (e.g., 1⃣ …)
- 
If you have no active alerts: “No active alerts yet.”
- 
If notifications are muted: a line “  Muted (notifications are paused).”

#### 3.2 Buttons on Home Screen
Home screen has exactly 3 buttons (each on its own row):
1.   New alert
o
Starts the creation wizard.
2.   My alerts
o
Opens the Manage screen.
3.   Mute all
o
Opens the Mute menu where you snooze notifications.

### 4) “  My alerts” — Manage Screen
The Manage screen is where you view, select, edit, pause/resume, delete, or open matches for
an alert.

**Images from page 23:**
![Page 23](img/p23_01.jpg)
![Page 23](img/p23_02.jpg)

---

#### 4.1 What You See
At the top:
- 
“My alerts (N)” where N is total number of alerts.
Then a numbered list of all alerts (enabled and paused):
- 
The selected alert is prefixed with “▶️”
- 
Each line shows:
o
alert number emoji (e.g., 2)
o
alert label
o
status icon:   if enabled,   if paused
o
mode title: e.g., “Once”, “Perpetually”, “Daily digest …”, “Weekly digest …”

Below the list there is a Selected section that displays the selected alert’s configuration in
detail:
- 
Scope (what is being tracked)
- 
Filters (each active filter shown as a bullet line)
- 
Expression (if the alert uses the expression builder)
- 
Conditions list (each condition numbered, with ranges/values)

#### 4.2 Selecting an Alert

**Images from page 24:**
![Page 24](img/p24_01.png)
![Page 24](img/p24_02.png)

---

There are two ways:
- 
Tap an alert line in the list (each line is a button) to select it.
- 
Or press Choose № to open a numeric picker (useful if you have many alerts).

if “Choose №”:

#### 4.3 Buttons in Manage Screen (Global Controls)
The Manage screen keyboard contains these controls:
1. ✏️ Edit
o
Opens the wizard pre-filled with the selected alert’s settings.
2. 🗑 Delete
o
Deletes the selected alert immediately.
3.   Pause / ▶️ Turn on
o
Toggles the selected alert ON/OFF (enabled ↔ paused).
4. ◀️ Prev
o
Selects the previous alert in the list.
5. Choose №
o
Opens numeric selection screen.
6. ▶️ Next
o
Selects the next alert in the list.
7. ⬅️ Back
o
Returns to Alerts Home.
8.   New alert
o
Starts a new alert wizard (without losing the current alert list).

**Images from page 25:**
![Page 25](img/p25_01.png)
![Page 25](img/p25_02.jpg)

---

### 5) “  Mute all” — Snooze Notifications Menu
Muting does not delete alerts; it temporarily stops sending alert notifications.

#### 5.1 What You See
The screen title: “Mute all alerts:” and buttons described below.
#### 5.2 Buttons
1.   Snooze for 1 hour
2.   Snooze until end of day
3.   Snooze for 1 week
4. ⏸ Mute until manually enabled
5.   Unmute
6. ⬅️ Back (returns to Alerts Home)

**Images from page 26:**
![Page 26](img/p26_01.jpg)
![Page 26](img/p26_02.jpg)

---

Time zone note: the bot uses MSK (Europe/Moscow) for certain “end of day” and digest
scheduling calculations.

### 6) Creating / Editing an Alert — The 5-Step Wizard
Press   New alert or ✏️ Edit to open the wizard.
The wizard has 5 steps:
1. Scope
2. Filters
3. Conditions & Expression
4. Trigger mode
5. Name
→ then Summary → Save

### Step 1 of 5 — Scope (What bonds to track)
Screen Purpose
Choose the bond universe:
- 
Specific bonds (manual tickers list)
- 
## Watchlist (your saved watchlist)
- 
Whole market (all bonds available)
- 
Cancel (exit wizard, go back)

If you pick “  Specific bonds”

**Images from page 27:**
![Page 27](img/p27_01.png)
![Page 27](img/p27_02.jpg)
![Page 27](img/p27_03.jpg)

---

The bot switches to an input screen where you must send tickers (one message) as text.
- 
Separate tickers by spaces or commas.
- 
The bot validates them and stores as the scope list.
- 
After successful entry, you continue to Step 2.

### Step 2 of 5 — Filters (Narrow the universe)
Screen Purpose
Filters constrain the scope before evaluating conditions.
The Step 2 screen displays:
- 
Title: Step 2 of 5
- 
Current Scope with number of currently applicable for the scope and applied filters bonds
(dynamic)
- 
A list of currently active filters (or “(none)”)
- 
Buttons to set filters or proceed.

Buttons in Step 2
1. ➡️ Continue (Moves to step 3)

**Images from page 28:**
![Page 28](img/p28_01.png)
![Page 28](img/p28_02.jpg)

---

2. Issue size
3. Sectors
4. Countries
5. Coupon per year
6. Par value
7. Put/Call option
8. Remove bonds
9.   Clear (clears all filters)
10. ⬅️ Back (returns to Step 1)
11.   Cancel (exits wizard)

#### 2.1 Range Filters (Issue size, Par value)
When you press Issue size or Par value, the bot asks you to type a numeric range.
Accepted formats:
- 
min-max (e.g., 100-500)
- 
>=min and >min (e.g., >=100 or >100)
- 
<=max and <max (e.g., <=500 or <500)
Buttons on the range input screen:
- 
⬅️ Back (returns to Step 2)
- 
Cancel (exits wizard)

#### 2.2 List Filters (Sectors, Countries, Coupon per year)
When you press Sectors, Countries, or Coupon per year, you get a checklist menu.
How it works:
- 
Each item toggles on/off.
- 
Selected items show a “ ”.
- 
Press Done to confirm and return to Step 2.
- 
Press ⬅️ Back to return to Step 2 without changing further.

**Images from page 29:**
![Page 29](img/p29_01.png)

---

#### 2.3 Put/Call Option (Toggle)
Put/Call option is a toggle filter with 3 states:
1. Any (no filter)
2. Exclude Put/Call bonds
3. Only Put/Call bonds
Users press the button repeatedly to cycle through these states.

**Images from page 30:**
![Page 30](img/p30_01.jpg)
![Page 30](img/p30_02.jpg)
![Page 30](img/p30_03.png)
![Page 30](img/p30_04.png)
![Page 30](img/p30_05.png)
![Page 30](img/p30_06.png)

---

#### 2.4 Remove bonds (Exclude tickers)
This lets the user provide a list of tickers that will be excluded even if they match.
The bot prompts the user to send tickers, similar to “Specific bonds”.

### Step 3 of 5 — Conditions & Logical Expression
This is the most important step: define what “matching” means.
Screen Purpose
The Step 3 screen shows:
- 
Current expression (with a cursor “|”)
- 
Conditions list (numbered)
- 
A keyboard to build logic and add/delete conditions.
The expression preview looks like:
C1 AND ( C2 OR C3 )
(with a cursor marker where insertion happens).

Buttons in Step 3 (Expression Keyboard)
Cursor movement
- 
⬅️ Move cursor left
- 
➡️ Move cursor right
Insert logic tokens

**Images from page 31:**
![Page 31](img/p31_01.png)
![Page 31](img/p31_02.jpg)
![Page 31](img/p31_03.jpg)

---

- 
( insert opening parenthesis
- 
) insert closing parenthesis
- 
AND
- 
OR
Condition controls
- 
Condition — add a new condition (opens parameter selection)
- 
⏸ Delete — delete token left of cursor
- 
Clear — clears the expression and all conditions
Navigation
- 
Continue — validates expression and goes to Step 4
- 
⬅️ Back — returns to Step 2
- 
Cancel — exits wizard

Expression Validation Rules (What “  Continue” checks)
When you press   Continue, the bot validates that:
- 
Parentheses are balanced
- 
Token order is logically valid
- 
Conditions referenced exist
If validation fails, it shows an error (e.g., “Invalid expression: …”).

Adding a Condition (  Condition flow)
Press   Condition → you enter a 2-stage selector:

**Images from page 32:**
![Page 32](img/p32_01.png)

---

1. Choose a parameter group (Yield, Duration, Ratings, etc.)
2. Choose a specific parameter within that group
Then enter a value/range or pick from a list (ratings).

Numeric conditions (range input)
For most numeric parameters you’ll be asked to type a range using the same formats as filters
(min-max, >=, <=, >, <).

Credit rating condition (list picker) – the only categorical value which serves as a
condition since credit ratings apart from sector, issue size, number of coupons per year
etc. is dynamic and can be changed, thus should be duly monitored.
If you choose a rating parameter, you get a list of ratings you can toggle and save.

**Images from page 33:**
![Page 33](img/p33_01.jpg)
![Page 33](img/p33_02.jpg)
![Page 33](img/p33_03.jpg)
![Page 33](img/p33_04.jpg)
![Page 33](img/p33_05.png)

---

### Step 4 of 5 — Trigger Mode (When to notify)
Screen Purpose
Choose how often you want notifications.
Buttons:
- 
Once (notify only when a bond becomes matching instantly)
- 
Perpetually (repeat notifications while matching, subject to cooldown of 5 minutes to
mitigate potential spam)
- 
Digest (send one digest per day or per week)
- 
⬅️ Back
- 
Cancel

Digest scheduling (Daily/Weekly)
If you select a digest trigger, you then configure:
- 
Daily vs Weekly (kind)

- 
For weekly: select weekdays

**Images from page 34:**
![Page 34](img/p34_01.jpg)
![Page 34](img/p34_02.jpg)
![Page 34](img/p34_03.jpg)
![Page 34](img/p34_04.jpg)

---

- 
Select time presets (09:00, 12:00, 18:00, 21:00) or enter custom HH:MM

- 
The time input is validated strictly as HH:MM format.

### Step 5 of 5 — Name Your Alert
Screen Purpose
Give the alert a friendly label.
Buttons:
- 
✏️ Enter name (switches to text input)
- 
➡️ Use default name (bot generates a name automatically)
- 
⬅️ Back
- 
Cancel

When entering a name:
- 
You type the name as a normal chat message.
- 
The bot saves it and proceeds to Summary.

Summary & Save
After Step 5, the bot shows a Summary page including:
- 
Name

**Images from page 35:**
![Page 35](img/p35_01.jpg)
![Page 35](img/p35_02.jpg)
![Page 35](img/p35_03.jpg)
![Page 35](img/p35_04.jpg)
![Page 35](img/p35_05.jpg)

---

- 
Scope
- 
Filters
- 
Expression / Conditions
- 
Trigger mode
Buttons:
- 
Save
- 
⬅️ Back
- 
Cancel

After saving:
- 
The alert is stored and becomes active (unless paused).
- 
The bot may perform an initial check and show bonds that match right now (paginated
view).

### 7) Alert Notifications & Digest Messages

**Images from page 36:**
![Page 36](img/p36_01.png)
![Page 36](img/p36_02.png)

---

#### 7.1 Instant Alert Notification (single bond)
When a bond triggers your alert, the bot sends a message like:
- 
Title: “  Alert: ”
- 
Bond link line (which is also clickable and transforms to terminal or t-invest. The transition
menu also contains important information on the bond)
- 
Matched conditions list (with values)
- 
Button: ✖️ Close (deletes the notification message)

#### 7.2 Digest / Initial Check Pagination Controls
Digest and initial-check lists are paginated with:
- 
◀️ previous page
- 
X/Y page indicator (non-action)
- 
▶️ next page
- 
Close (deletes digest message)

**Images from page 37:**
![Page 37](img/p37_01.png)
![Page 37](img/p37_02.jpg)
![Page 37](img/p37_03.jpg)

---

### 8) Troubleshooting & Tips
“Invalid expression”
- 
Ensure parentheses match.
- 
Ensure you did not leave dangling operators (e.g., “C1 AND” at the end).
- 
Ensure conditions exist before referencing them (don’t delete a condition token still used).
Too many notifications
- 
Use Mute all temporarily.
- 
Switch to Daily digest or Weekly digest.
- 
Use stronger filters or narrower scope.
Digest time not matching your local time
- 
Digest scheduling is based on MSK (Europe/Moscow) in code.

**Images from page 38:**
![Page 38](img/p38_01.png)

---

## Watchlist (/watchlist, /wl) and Blacklist (/blacklist, /bl) functions
### 1) What Watchlist and Blacklist are (and why you’d use them)
## Watchlist ( )
Purpose: A personal “favorites” list of bond tickers you want to track.
What it’s used for:
- 
Alerts scope: You can build alerts that monitor only your Watchlist (instead of the whole
market or a custom set).
- 
Screener view: You can switch the screener to show Watchlist-only (so you don’t see
everything else).
Blacklist ( )
Purpose: A personal “exclude” list of bond tickers you don’t want to see in the screener.
What it’s used for:
- 
Screener cleanup: Removing unwanted bonds from the screener results (e.g., low liquidity,
irrelevant issuers, duplicates).
What both lists “contain”
Both Watchlist and Blacklist store bond tickers (text codes like SU26238RMFS7).

### 2) How to open Watchlist / Blacklist panels
Watchlist
Send:
- 
/watchlist (or short alias /wl)
Blacklist
Send:
- 
/blacklist (or short alias /bl)
After you send the command, the bot posts a panel message with:
- 
A header (“  Watchlist” or “  Blacklist”)
- 
A paged list (up to 23 items per page)
- 
Buttons for navigation + Add/Remove/Close

**Images from page 39:**
![Page 39](img/p39_01.jpg)

---

### 3) How the list panel is laid out (what you’re looking at)
Inside the panel, bonds are shown as:
- 
A clickable ticker link (opens the bond page) plus the bond’s display name
- 
Grouped by sector (when sector data exists), with a section header like   SectorName: (N)
- 
A page footer like — Page X/Y —
If there’s nothing to show, the panel displays (empty).

**Images from page 40:**
![Page 40](img/p40_01.jpg)
![Page 40](img/p40_02.png)

---

### 4) Every button explained
The Watchlist and Blacklist panels use the same button layout.
Navigation row
- 
⏸ — go to first page
- 
◀️ / ◀️ — go to previous page
- 
X/Y — page indicator
- 
▶️ / ▶️ — go to next page
- 
⏸ — go to last page

Action row
- 
Add — start adding tickers to this list
- 
Remove — start removing tickers from this list
- 
Close — closes the panel and cleans up messages

### 5) Adding bonds (step-by-step)
Works the same for Watchlist and Blacklist.
### Step 1 — Click   Add
The bot switches into “input mode” and sends a prompt:
“Send tickers. Separator: space or comma. To cancel — /cancel”

### Step 2 — Type tickers and send
You can enter multiple tickers separated by:
- 
spaces, or
- 
commas
Examples:
- 
SU26238RMFS7 SU26239RMFS5

**Images from page 41:**
![Page 41](img/p41_01.png)
![Page 41](img/p41_02.png)

---

- 
SU26238RMFS7, SU26239RMFS5
Tickers are automatically uppercased internally.
### Step 3 — Bot confirms and refreshes the panel
- 
If something changed. Bonds are added to the list.
- 
If everything you entered was already there: No changes.
Then the bot updates the list panel so you immediately see the result.
Also, the bot will delete:
- 
your ticker message, and
- 
the prompt message
to keep the chat clean.

### 6) Removing bonds (step-by-step)
Works the same for Watchlist and Blacklist.
### Step 1 — Click   Remove
You’ll get the same style prompt (space/comma-separated tickers; cancel via /cancel).

### Step 2 — Type tickers you want removed and send
Same formatting rules as Add.
### Step 3 — Bot confirms and refreshes the panel
- 
Removed: ... or No changes.
Then it refreshes the list panel.

### 7) Canceling Add/Remove mode
If you clicked Add or Remove but changed your mind:
Send:
- 
/cancel
The bot exits input mode and removes the input prompt.

### 8) Closing the Watchlist/Blacklist panel
Click:
- 
Close
What it does:
- 
Deletes the panel message and also removes the original command message (the /wl or
/bl message), so your chat stays tidy.

**Images from page 42:**
![Page 42](img/p42_01.png)

---

### 9) Using Watchlist in the Screener (Watchlist-only mode)
Where it is
In the screener, open Settings and find the Watchlist toggle:
- 
It appears as “Watchlist / All list” (label changes depending on state).

What it does
- 
When switched to Watchlist mode, the screener is intended to show only bonds from your
Watchlist.
- 
When switched back, the screener shows the full universe again (with your usual
filters/sort/group).

### 10) Using Blacklist in the Screener (removing bonds)
Adding bonds to your Blacklist will exclude bonds from the panel of the screener.

### 11) Watchlist vs Blacklist in Alerts (the important difference)
Watchlist is an Alerts scope
When creating an alert, the scope selection includes:
- 
## Watchlist (alongside “Specific bonds” and “Whole market”).
If you choose Watchlist scope, the alert evaluates only bonds from your Watchlist.

**Images from page 43:**
![Page 43](img/p43_01.png)
![Page 43](img/p43_02.png)

---

Blacklist does NOT affect alerts
Even when an alert scope is “Current screener”, the system explicitly ignores blacklist during
alert evaluation. If some specific bonds should be excluded from the scope, button Remove
bonds from the filter group should be used.

### 12) Practical notes & troubleshooting
### 1) “I added a ticker but I don’t see it in the list”
The list display is built from known tickers; the formatter filters through a ticker→UID mapping
before it renders items.
So if a ticker is misspelled or not recognized, it may not appear as a clickable bond entry.
What to do:
- 
Re-check spelling
- 
Try searching/opening the bond elsewhere in the bot first, then re-add
### 2) “Why did my message disappear after I entered tickers?”
That’s expected: the bot will delete your input message and the prompt to keep the chat clean.
### 3) “How do I undo a mistake quickly?”
- 
Use   Remove in the list panel, or
- 
open /bl and remove from Blacklist, or
- 
open /wl and remove from Watchlist
### 4) “How many items per page?”
Lists render with a page cap of 23 bonds per page.

**Images from page 44:**
![Page 44](img/p44_01.jpg)
