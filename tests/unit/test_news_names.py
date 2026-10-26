"""
core/news_archive.py: naming a company in a headline (decision 77,
research/headline-tagging-2026-10-26.md).

LABELLED is every story the archive's own search for a symbol found, that
Yahoo tagged with nothing, and whose title named the symbol's company by the
measured rule, as of 2026-10-26: one row per (symbol, title), with the name
the registry carries and a hand label. ABOUT: the story is about the
company. MENTION: it names it in passing. WRONG: another entity.
"""

import pytest

from core.news_archive import (
    company_names,
    is_distinctive,
    name_forms,
    name_patterns,
    names_company,
)

ABOUT, MENTION, WRONG = "about", "mention", "wrong"

LABELLED = [
    (
        "APT-USD",
        "Aptos",
        "Aptos crosses 4.5 billion total transactions as daily activity hits record",
        ABOUT,
    ),
    ("APT-USD", "Aptos", "Aptos launches Confidential APT with opt-in privacy", ABOUT),
    (
        "ATOM-USD",
        "Cosmos Hub",
        "Cosmos Hub Voters Are Vetoing a Proposal to Refund the Neutron Attacker 1.2 Million ATOM",
        ABOUT,
    ),
    (
        "AVY",
        "Avery Dennison Corp",
        "Avery Dennison Q2 Earnings Beat on Pricing & Productivity Gains",
        ABOUT,
    ),
    (
        "BG",
        "Bunge Global SA",
        "Bunge Global (BG) Leans on Viterra Synergies and Buybacks Is Its Capital Strategy Evolving?",
        ABOUT,
    ),
    (
        "BTC-USD",
        "Bitcoin",
        "If Elon Musk gave kids Bitcoin instead of SpaceX shares, here's how much they would get once they turn 18",
        MENTION,
    ),
    (
        "CRH",
        "CRH PUBLIC LTD CO",
        "Barclays Adjusts Price Target on CRH public to $110 From $131",
        ABOUT,
    ),
    (
        "CRH",
        "CRH PUBLIC LTD CO",
        "Deutsche Bank Adjusts Price Target on CRH public to $118 From $128",
        ABOUT,
    ),
    (
        "CRH",
        "CRH PUBLIC LTD CO",
        "Rothschild & Co Redburn Adjusts Price Target on CRH public to $120 From $135, Keeps Buy Rating",
        ABOUT,
    ),
    (
        "CRH",
        "CRH PUBLIC LTD CO",
        "Stephens Cuts Price Target on CRH public to $105 From $125, Keeps Overweight Rating",
        ABOUT,
    ),
    (
        "CRH",
        "CRH PUBLIC LTD CO",
        "UBS Adjusts Price Target on CRH public to $135 From $147, Maintains Buy Rating",
        ABOUT,
    ),
    ("CRO-USD", "Cronos", "Cronos (CRON) Q2 2026 Earnings Call Transcript", WRONG),
    ("CRO-USD", "Cronos", "Cronos Closes Acquisition of CanAdelaar", WRONG),
    ("CRO-USD", "Cronos", "Cronos Stock Is Getting New Life as Shares Hit a 6-Month High", WRONG),
    (
        "DOGE-USD",
        "Dogecoin",
        "Bitwise Kills Dogecoin ETF After 10 Months — Is the $1 DOGE Dream Dead?",
        ABOUT,
    ),
    (
        "DOGE-USD",
        "Dogecoin",
        "Dogecoin Jumps to Highest Price in Months: Here's What's Going On",
        ABOUT,
    ),
    (
        "DOGE-USD",
        "Dogecoin",
        "Dogecoin Price Prediction: $0.07 Support Loss Could Cause a 19% Drop for DOGE",
        ABOUT,
    ),
    ("DOGE-USD", "Dogecoin", "Dogecoin co-founder mocks Trump's $5,000 dividend plan", ABOUT),
    ("DOGE-USD", "Dogecoin", "Elon Musk is finally putting Dogecoin on the moon today", ABOUT),
    (
        "DOGE-USD",
        "Dogecoin",
        "You bought Dogecoin instead of Nike 5 years ago? Here's what happened",
        MENTION,
    ),
    (
        "DOT-USD",
        "Polkadot",
        "Polkadot FX Launches a New Take on Functional Beverages With Spark, Connect and Revive",
        WRONG,
    ),
    ("ENA-USD", "Ethena", "Ethena Fee Switch Vote Ties ENA Buybacks to USDe Growth", ABOUT),
    ("ENA-USD", "Ethena", "Ethena and FalconX: Institutionalizing the USDe Yield Pipeline", ABOUT),
    (
        "ETR",
        "ENTERGY CORP /DE/",
        "Entergy (ETR) Beat Estimates And Reaffirmed Guidance, Is The Stock Still Cheap?",
        ABOUT,
    ),
    (
        "FERG",
        "Ferguson Enterprises Inc. /DE/",
        "Ferguson Enterprises (FERG) Is Up 10.1% After Joining The S&P 500 Index - What's Changed",
        ABOUT,
    ),
    (
        "GWW",
        "W.W. GRAINGER, INC.",
        "How Investors May Respond To W.W. Grainger (GWW) Expanding Its Pacific Northwest Distribution Network",
        ABOUT,
    ),
    (
        "HCA",
        "HCA Healthcare, Inc.",
        "HCA Healthcare Foundation Awards $25,000 to Waypoint to Expand the Network of Adults Supporting Youth Mental Wellness Across Virginia",
        ABOUT,
    ),
    (
        "HCA",
        "HCA Healthcare, Inc.",
        "MEDITECH Expanse EHR Live Across First Full U.S. Divisions of HCA Healthcare",
        ABOUT,
    ),
    (
        "HCA",
        "HCA Healthcare, Inc.",
        "SOLIS MAMMOGRAPHY AND HCA HEALTHCARE CHARLESTON PARTNER TO EXPAND BREAST HEALTH SERVICES IN SOUTH CAROLINA",
        ABOUT,
    ),
    (
        "HYPE-USD",
        "Hyperliquid",
        "House Oversight Targets Hyperliquid, Crypto.com and PredictIt in Insider Trading Probe",
        ABOUT,
    ),
    (
        "HYPE-USD",
        "Hyperliquid",
        "Hyperliquid Data Now Live on Bloomberg Terminal as Trump Working to Bring The Platform to The US",
        ABOUT,
    ),
    (
        "HYPE-USD",
        "Hyperliquid",
        "Hyperliquid Labs Is Registered in Singapore, but the Blockchain Is a Different Story",
        ABOUT,
    ),
    (
        "ICP-USD",
        "Internet Computer",
        "Internet Computer (ICP) Holders Face 2 Opposing Signals. Here’s What Each One Says",
        ABOUT,
    ),
    (
        "JNJ",
        "JOHNSON & JOHNSON",
        "Johnson & Johnson Reports 75% Tumor Response Rate for JNJ-6143 in Advanced Prostate Cancer",
        ABOUT,
    ),
    (
        "KCS-USD",
        "KuCoin",
        "AI Scams are Surging in Crypto. KuCoin is Betting on a New Security Standard",
        ABOUT,
    ),
    (
        "KCS-USD",
        "KuCoin",
        "Beyond the Podium: How KuCoin Is Turning Long-Term Commitment into Lasting Impact",
        ABOUT,
    ),
    (
        "KCS-USD",
        "KuCoin",
        "Korean Watchdog Wins Approval to Block KuCoin, MEXC and 25 More Unregistered Crypto Sites",
        ABOUT,
    ),
    (
        "LINK-USD",
        "Chainlink",
        "Deepseek AI Predicts That Chainlink (LINK) is About to do Something Huge",
        ABOUT,
    ),
    (
        "MGM",
        "MGM Resorts International",
        "Pro Padel League Secures Landmark Title Sponsorship with MGM Resorts International for Las Vegas Tournament November 18 - 21",
        MENTION,
    ),
    (
        "MKTX",
        "MARKETAXESS HOLDINGS INC",
        "MarketAxess (MKTX) Rockets on Merger Deal at 33% Premium",
        ABOUT,
    ),
    (
        "ONDO-USD",
        "Ondo",
        "ONDO Expands Into Global Performance Apparel With Acquisition of Champion System",
        WRONG,
    ),
    ("ONDO-USD", "Ondo", "Ondo Perps breaks past $300M in 24-hour volume milestone", ABOUT),
    ("ONDO-USD", "Ondo", "Ondo Perps hits $7 billion in volume weeks after launch", ABOUT),
    ("ONDO-USD", "Ondo", "Ondo makes its case to regulators across 3 separate filings", ABOUT),
    ("ONDO-USD", "Ondo", "Ondo's USDY goes live on BNB Chain with instant minting", ABOUT),
    (
        "ONDO-USD",
        "Ondo",
        "Who Controls Ondo? Inside the Mega Succession War Shaking Crypto’s RWA Giant",
        ABOUT,
    ),
    (
        "PDD",
        "PDD Holdings Inc.",
        "PDD Holdings Q2 2026 earnings: profit falls 12%, beats estimates",
        ABOUT,
    ),
    (
        "PDD",
        "PDD Holdings Inc.",
        "PDD Holdings Shares Rise After Second-Quarter Earnings Beat",
        ABOUT,
    ),
    (
        "PDD",
        "PDD Holdings Inc.",
        "PDD Holdings’ net profit falls 12% despite revenue growth in Q2",
        ABOUT,
    ),
    (
        "PENGU-USD",
        "Pudgy Penguins",
        "Meet Your New Boba Date! Pudgy Penguins Waddles Into Kung Fu Tea for a Limited-Time Collaboration",
        ABOUT,
    ),
    (
        "PEPE-USD",
        "Pepe",
        'Hyperquake Welcomes Barcelona-Based Jose "Pepe" Gorbea as VP of International Growth & Development',
        WRONG,
    ),
    (
        "PI-USD",
        "Pi Network",
        "Pi Network's Latest SoloHost Update Skips the One Thing PI Still Needs",
        ABOUT,
    ),
    (
        "SHIB-USD",
        "Shiba Inu",
        "Shiba Inu Says Bears “Chose Cardio,” but SHIB Lagged Most of the Market",
        ABOUT,
    ),
    (
        "SKYD",
        "Skydance Corp",
        "SKYD Stock Alert: Elon Musk Reportedly Makes $1 Billion Skydance Investment",
        ABOUT,
    ),
    (
        "SKYD",
        "Skydance Corp",
        "SKYD Stock Edges Up Overnight After Worst Day In Over 5 Months — Skydance Clarifies Warrant Price Adjustment",
        ABOUT,
    ),
    (
        "SKYD",
        "Skydance Corp",
        "SKYD Stock Gets JPMorgan Upgrade: Analyst Sees Skydance Gaining Ground On Netflix, Disney With Warner Bros. Deal",
        ABOUT,
    ),
    (
        "SKYD",
        "Skydance Corp",
        "SKYD Stock Slips Premarket: Morningstar Says Skydance's Streaming Ambitions Face A $12 Valuation And Heavy Debt Burden",
        ABOUT,
    ),
    (
        "SKYD",
        "Skydance Corp",
        "The Sky Is Falling for Skydance Stock as Financial Concerns Mount. How to Play SKYD Here.",
        ABOUT,
    ),
    ("SOL-USD", "Solana", "Morning Minute: Solana Is About To Get Faster", ABOUT),
    (
        "STX-USD",
        "Stacks",
        "401(k) Savings Benchmarks for Millennials: How Your Retirement Fund Stacks Up",
        WRONG,
    ),
    (
        "STX-USD",
        "Stacks",
        "Here's How Social Security's 2027 COLA Stacks Up Against Past COLAs",
        WRONG,
    ),
    (
        "STX-USD",
        "Stacks",
        "Here's How the 2027 Social Security Cost-of-Living Adjustment (COLA) Stacks Up Against 2024, 2025, and 2026",
        WRONG,
    ),
    (
        "STX-USD",
        "Stacks",
        "How Median Net Worth for Americans Ages 55-64 Stacks Up, According to Federal Reserve Data",
        WRONG,
    ),
    (
        "STX-USD",
        "Stacks",
        "Is the 5% Withdrawal Rule Safe? Here’s How It Stacks Up Against 4%",
        WRONG,
    ),
    (
        "STX-USD",
        "Stacks",
        "O'Reilly Launches Expert Intelligence, Bringing Vetted Practitioner Knowledge Directly into Enterprise AI Stacks and Providing the Skills Framework to Apply It",
        WRONG,
    ),
    (
        "STX-USD",
        "Stacks",
        "Pilot-Era Agentic AI Stacks Expose Enterprises to Integration and Governance Risks, Finds Info-Tech Research Group",
        WRONG,
    ),
    (
        "STX-USD",
        "Stacks",
        "Retirees in One State Have Just $35,000 Saved For Retirement, The Lowest In The Country — See How Yours Stacks Up",
        WRONG,
    ),
    (
        "STX-USD",
        "Stacks",
        "Social Security Benefits at Age 65: How the Average Payment Stacks Up and What It Means for Retirees",
        WRONG,
    ),
    (
        "STX-USD",
        "Stacks",
        "The Average Social Security Check Is $2,086 in 2026. Here’s How Yours Stacks Up",
        WRONG,
    ),
    (
        "STX-USD",
        "Stacks",
        "UK High Bandwidth Memory Market Report 2026: Set for Strong Growth Through 2034, Fueled by AI, Advanced Computing and Rising Demand for High-Capacity Memory Stacks",
        WRONG,
    ),
    (
        "STX-USD",
        "Stacks",
        "vCluster Labs Ships Stacks to Package Any AI Environment as a Managed Service",
        WRONG,
    ),
    (
        "TKO",
        "TKO Group Holdings, Inc.",
        "TKO Group Holdings Insider Sold Shares Worth $1,890,631, According to a Recent SEC Filing",
        ABOUT,
    ),
    (
        "TT",
        "Trane Technologies plc",
        "Trane Technologies to Acquire Heat-Transfer Products Maker Cancoil Thermal",
        ABOUT,
    ),
    (
        "UBER",
        "Uber Technologies, Inc",
        "Instamall Launches an Uber Eats-like Concept for Fashion in Miami, Next Stop a Nationwide Rollout",
        MENTION,
    ),
    (
        "UBER",
        "Uber Technologies, Inc",
        "John Lewis expands UK rapid delivery through Deliveroo and Uber Eats",
        MENTION,
    ),
    (
        "UBER",
        "Uber Technologies, Inc",
        "Uber Rolling Out ChatGPT Work to More Than 30,000 Employees",
        ABOUT,
    ),
    (
        "UBER",
        "Uber Technologies, Inc",
        "Uber Rolls ChatGPT Work to All 30,000 Employees: The Unpriced-Labor Benchmark",
        ABOUT,
    ),
    ("UBER", "Uber Technologies, Inc", "Uber puts all 30,000 employees on ChatGPT Work", ABOUT),
    (
        "ULTA",
        "Ulta Beauty, Inc.",
        '"From K-Beauty to K-Patch": MARSHIQUE Expands into North America with Launch at Ulta Beauty',
        MENTION,
    ),
    (
        "ULTA",
        "Ulta Beauty, Inc.",
        "&honey Brings Its Japanese Haircare Rituals to Ulta Beauty",
        MENTION,
    ),
    (
        "ULTA",
        "Ulta Beauty, Inc.",
        "COSRX Brings Its 5 Peptide Lip Collection to Ulta Beauty Stores Offline",
        MENTION,
    ),
    (
        "ULTA",
        "Ulta Beauty, Inc.",
        "How Ulta Beauty Fosters Togetherness — at Every Organizational Level — to Win",
        ABOUT,
    ),
    (
        "ULTA",
        "Ulta Beauty, Inc.",
        "Status Summit Brings Together Leaders from Ulta Beauty, Google, and Liquid Death on How AI Is Redefining Brand Discovery",
        MENTION,
    ),
    (
        "ULTA",
        "Ulta Beauty, Inc.",
        "Wavytalk Expands Ulta Beauty Partnership with Nationwide In-Store Debut",
        MENTION,
    ),
    (
        "UNI-USD",
        "Uniswap",
        "Morning Minute: Uniswap Enters the Launchpad Wars on Robinhood Chain",
        ABOUT,
    ),
    ("VRSN", "VERISIGN INC/CA", "VeriSign (VRSN) Q3 2026 Earnings Call Transcript", ABOUT),
    (
        "WLD-USD",
        "Worldcoin",
        "Sam Altman’s Worldcoin: How VanEck, MrBeast, and Wall Street Capitalize on Exploited Biometrics",
        ABOUT,
    ),
    (
        "WLD-USD",
        "Worldcoin",
        "South Korea's AI Mania Spills Into Crypto, With Worldcoin as Top Pick",
        ABOUT,
    ),
    (
        "WLFI-USD",
        "World Liberty Financial",
        "Ex-Circle Engineer Takes Trump to Court Over Emirati Payments to World Liberty Financial",
        ABOUT,
    ),
    (
        "WLFI-USD",
        "World Liberty Financial",
        "Justin Sun Wins Key Court Battle Against Trump-Backed World Liberty Financial",
        ABOUT,
    ),
    (
        "WLFI-USD",
        "World Liberty Financial",
        "Trump-Linked World Liberty Financial Wins Historic US Bank Approval, Fueling Conflict-of-Interest Fears",
        ABOUT,
    ),
    (
        "WLFI-USD",
        "World Liberty Financial",
        "US regulator approves bank charter for Trump-backed crypto company World Liberty Financial",
        ABOUT,
    ),
    (
        "WLFI-USD",
        "World Liberty Financial",
        "World Liberty Financial USD1 Plan Clears Initial Hurdle for National Trust Bank",
        ABOUT,
    ),
    (
        "XMR-USD",
        "Monero",
        "Doctor Profit Warns of a Very Big Monero (XMR) Liquidation Event",
        ABOUT,
    ),
    ("ZEC-USD", "Zcash", "Zcash Sprout Holders Have Two Weeks to Move Their ZEC", ABOUT),
    (
        "ZRO-USD",
        "LayerZero",
        "Kelp DAO Sues LayerZero and CEO Bryan Pellegrino Over $292 Million rsETH Bridge Exploit",
        ABOUT,
    ),
    (
        "ZRO-USD",
        "LayerZero",
        "Morning Minute: LayerZero Announces ATLAS as New Settlement Engine",
        ABOUT,
    ),
]


def rescued(name, title):
    return names_company(title, name_patterns([name]))


@pytest.mark.parametrize(
    ("symbol", "name", "title", "label"), [r for r in LABELLED if r[1] != "Stacks"]
)
def test_every_labelled_story_is_rescued_by_its_name(symbol, name, title, label):
    """Wrong ones included: Cronos the cannabis company is a known false positive."""
    assert rescued(name, title)


@pytest.mark.parametrize("title", [r[2] for r in LABELLED if r[1] == "Stacks"])
def test_a_plural_dictionary_word_rescues_nothing(title):
    """'How yours stacks up': 12 of the 12 Stacks matches were not the coin."""
    assert not rescued("Stacks", title)


def test_the_labels_of_what_the_rule_keeps():
    """Pinned: a change to the rule or the labels must restate the note's numbers."""
    kept = [r[3] for r in LABELLED if rescued(r[1], r[2])]
    assert (kept.count(ABOUT), kept.count(MENTION), kept.count(WRONG)) == (69, 10, 6)


@pytest.mark.parametrize(
    ("form", "expected"),
    [
        ("PDD Holdings", True),
        ("Skydance", True),
        ("Target", False),  # "price target"
        ("Stacks", False),  # plural of a dictionary word
        ("PDD", False),  # short acronym
        ("Dow", False),
        ("KuCoin", True),
    ],
)
def test_distinctive_forms(form, expected):
    assert is_distinctive(form) is expected


def test_name_forms_drop_legal_form_state_tag_and_generic_tail():
    assert name_forms("PDD Holdings Inc.") == {"PDD Holdings", "PDD"}
    assert name_forms("VERISIGN INC/CA") == {"VERISIGN"}
    assert name_forms("The Campbell's Company") == {"Campbell's"}


def test_company_names_by_asset_class():
    assert company_names("PDD", "equity", "PDD Holdings Inc.") == ["PDD Holdings Inc."]
    assert company_names("KEY", "equity", "KEYCORP /NEW/") == ["KEYCORP /NEW/", "KEYCORP"]
    assert company_names("CRO-USD", "crypto", None, "Cronos", "crypto") == ["Cronos"]
    # A stablecoin's or tokenized asset's name names its issuer, not it.
    assert company_names("XAUT-USD", "crypto", None, "Tether Gold", "tokenized") == []
    assert company_names("SPY", "etf", None, "SPDR S&P 500") == []


def test_a_name_inside_a_word_is_no_match():
    assert not rescued("Uber Technologies, Inc", "Exuberant markets")
    assert rescued("Uber Technologies, Inc", "Uber's quarter")
