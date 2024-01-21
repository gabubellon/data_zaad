import spotipy

class Spotify_API():

    def __init__(self, client_id, client_secret,redirect_uri):
      self.client_id = client_id
      self.client_secret = client_secret
      self.redirect_uri = redirect_uri
      self.spotify_session = self._spotify_auth()

    def _spotify_auth(self):
        # client_id = Variable.get("spotify_client_id")
        # client_secret = Variable.get("spotify_client_secret")
        # redirect_uri = Variable.get("spotify_redirect_uri")

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

    def spotify_search(self,search_term, limit, return_type, market,return_columns):

        search_result = self.spotify_session.search(q=search_term, limit=limit, type=return_type,market=market)

        return_list = []

        for item in search_result.get("shows").get("items"):
            if item:
                return_list.append({col: item.get(col) for col in return_columns})

        return return_list


    def spotify_show_episodes(self,show_id, return_columns):

        return_list = []
        episodes_return = self.spotify_session.show_episodes(show_id=show_id, offset=0)

        while episodes_return:
            dh_ep = episodes_return.get("items")

            for item in dh_ep:
                return_list.append({col: item.get(col) for col in return_columns})

            if not episodes_return["next"]:
                break

            episodes_return = self.spotify_session.next(episodes_return)

        return return_list