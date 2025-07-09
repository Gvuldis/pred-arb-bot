'''
Focus keywords for Bodega-Polymarket matching.
List of popular teams, events, and specific entities used to boost fuzzy matching.
'''

# NFL Teams
NFL_TEAMS = [
    "Arizona Cardinals", "Atlanta Falcons", "Baltimore Ravens", "Buffalo Bills",
    "Carolina Panthers", "Chicago Bears", "Cincinnati Bengals", "Cleveland Browns",
    "Dallas Cowboys", "Denver Broncos", "Detroit Lions", "Green Bay Packers",
    "Houston Texans", "Indianapolis Colts", "Jacksonville Jaguars", "Kansas City Chiefs",
    "Las Vegas Raiders", "Los Angeles Chargers", "Los Angeles Rams", "Miami Dolphins",
    "Minnesota Vikings", "New England Patriots", "New Orleans Saints", "New York Giants",
    "New York Jets", "Philadelphia Eagles", "Pittsburgh Steelers", "San Francisco 49ers",
    "Seattle Seahawks", "Tampa Bay Buccaneers", "Tennessee Titans", "Washington Commanders"
]

# NBA Teams
NBA_TEAMS = [
    "Atlanta Hawks", "Boston Celtics", "Brooklyn Nets", "Charlotte Hornets",
    "Chicago Bulls", "Cleveland Cavaliers", "Dallas Mavericks", "Denver Nuggets",
    "Detroit Pistons", "Golden State Warriors", "Houston Rockets", "Indiana Pacers",
    "Los Angeles Clippers", "Los Angeles Lakers", "Memphis Grizzlies", "Miami Heat",
    "Milwaukee Bucks", "Minnesota Timberwolves", "New Orleans Pelicans", "New York Knicks",
    "Oklahoma City Thunder", "Orlando Magic", "Philadelphia 76ers", "Phoenix Suns",
    "Portland Trail Blazers", "Sacramento Kings", "San Antonio Spurs", "Toronto Raptors",
    "Utah Jazz", "Washington Wizards"
]

# MLB Teams
MLB_TEAMS = [
    "Arizona Diamondbacks", "Atlanta Braves", "Baltimore Orioles", "Boston Red Sox",
    "Chicago Cubs", "Chicago White Sox", "Cincinnati Reds", "Cleveland Guardians",
    "Colorado Rockies", "Detroit Tigers", "Houston Astros", "Kansas City Royals",
    "Los Angeles Angels", "Los Angeles Dodgers", "Miami Marlins", "Milwaukee Brewers",
    "Minnesota Twins", "New York Mets", "New York Yankees", "Oakland Athletics",
    "Philadelphia Phillies", "Pittsburgh Pirates", "San Diego Padres", "San Francisco Giants",
    "Seattle Mariners", "St. Louis Cardinals", "Tampa Bay Rays", "Texas Rangers",
    "Toronto Blue Jays", "Washington Nationals"
]

# NHL Teams
NHL_TEAMS = [
    "Anaheim Ducks", "Arizona Coyotes", "Boston Bruins", "Buffalo Sabres",
    "Calgary Flames", "Carolina Hurricanes", "Chicago Blackhawks", "Colorado Avalanche",
    "Columbus Blue Jackets", "Dallas Stars", "Detroit Red Wings", "Edmonton Oilers",
    "Florida Panthers", "Los Angeles Kings", "Minnesota Wild", "Montreal Canadiens",
    "Nashville Predators", "New Jersey Devils", "New York Islanders", "New York Rangers",
    "Ottawa Senators", "Philadelphia Flyers", "Pittsburgh Penguins", "San Jose Sharks",
    "Seattle Kraken", "St. Louis Blues", "Tampa Bay Lightning", "Toronto Maple Leafs",
    "Vancouver Canucks", "Vegas Golden Knights", "Washington Capitals", "Winnipeg Jets"
]

# Premier League Clubs
PREMIER_LEAGUE = [
    "Arsenal", "Aston Villa", "Bournemouth", "Brentford", "Brighton & Hove Albion",
    "Chelsea", "Crystal Palace", "Everton", "Fulham", "Liverpool", "Luton Town",
    "Manchester City", "Manchester United", "Newcastle United", "Nottingham Forest",
    "Sheffield United", "Tottenham Hotspur", "West Ham United", "Wolverhampton Wanderers", "Burnley"
]

# Other Major Sports & Events
OTHER_SPORTS = [
    # Tennis Majors
    "Wimbledon", "US Open", "French Open", "Australian Open",
    # Motorsport
    "Formula 1", "MotoGP", "IndyCar", "NASCAR",
    # Golf Majors
    "The Masters", "PGA Championship", "U.S. Open Golf", "The Open Championship",
    # Boxing / MMA
    "UFC", "Bellator", "WBC", "WBA", "IBF",
    # Elections & Politics
    "Presidential Election", "Midterm Election", "Senate", "Congress", "Ballot Initiative",
    # Crypto
    "Bitcoin", "Ethereum", "Cardano", "Solana", "TRON", "Polkadot", "Dogecoin", "Shiba Inu",
    # Other
    "Cricket", "Rugby", "Olympics", "NBA Finals", "Super Bowl", "World Series", "Stanley Cup"
]

# Combine all focus keywords
FOCUS_KEYWORDS = (
    NFL_TEAMS + NBA_TEAMS + MLB_TEAMS + NHL_TEAMS + PREMIER_LEAGUE + OTHER_SPORTS
)

if __name__ == "__main__":
    print(f"Loaded {len(FOCUS_KEYWORDS)} focus keywords.")
