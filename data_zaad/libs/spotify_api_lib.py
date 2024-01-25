import spotipy


class Spotify_API:
    """Wrapper Class to connect on SpotifyAPI

    Use spotity (https://spotipy.readthedocs.io/en/2.22.1/) lib to connect and use
    Spotify Web API https://developer.spotify.com/documentation/web-api

    """

    def __init__(self, client_id, client_secret, redirect_uri):
        """Inicialize a spotify session
        Args:
            client_id (str):
                Spotify App client ID
            client_secret (str):
                Spotify App client secret
            redirect_uri ():
                Spotify App redirect url
        """

        self.client_id = client_id
        self.client_secret = client_secret
        self.redirect_uri = redirect_uri
        self.spotify_session = self._spotify_auth()

    def _spotify_auth(self):
        """Make Authentication o Spotify App using Spotify Auth code and token"""
        client_credentials_manager = spotipy.oauth2.SpotifyClientCredentials(
            self.client_id, self.client_secret
        )

        oauth_manager = spotipy.oauth2.SpotifyOAuth(
            client_id=self.client_id,
            client_secret=self.client_secret,
            redirect_uri=self.redirect_uri,
            scope="user-library-read",
        )

        return spotipy.Spotify(
            client_credentials_manager=client_credentials_manager,
            oauth_manager=oauth_manager,
        )

    def spotify_search(self, search_term, limit, return_type, market, return_columns):
        """Make a Sportify API search 
        (https://developer.spotify.com/documentation/web-api/reference/search)

        Args:
            search_term (str):
                Term to be used on search.
            limit (integer):
                Limit of data to return on search. Min: 0, Max: 50.
            return_type (string):
                Item type to search across.
                Allowed values:
                     "album", "artist", "playlist", "track", "show", "episode", "audiobook"
            market (string):
                An ISO 3166-1 alpha-2 country code.
                If a country code is specified, only content that is available in that market 
                will be returned.
            return_columns (list[str]):
                A list of columns name of each search to return from items data

        Returns:
            list[dict]: Return a list of dicts with the return_columns values
        """

        search_result = self.spotify_session.search(
            q=search_term, limit=limit, type=return_type, market=market
        )

        return_list = []

        for item in search_result.get("shows").get("items"):
            if item:
                return_list.append({col: item.get(col) for col in return_columns})

        return return_list

    def spotify_show_episodes(self, show_id, return_columns):
        """Get a Spotify Podcast show episodes

        Args:
            show_id (str): 
                Spotify Show ID
             return_columns (list[str]):
                A list of columns name of each search to return from items data

        Returns:
            list[dict]: Return a list of dicts with the return_columns values
        """

        return_list = []
        episodes_return = self.spotify_session.show_episodes(show_id=show_id, offset=0)

        while episodes_return:
            dh_ep = episodes_return.get("items")

            for item in dh_ep:
                if item:
                    return_list.append({col: item.get(col) for col in return_columns})

            if not episodes_return["next"]:
                break

            episodes_return = self.spotify_session.next(episodes_return)

        return return_list
