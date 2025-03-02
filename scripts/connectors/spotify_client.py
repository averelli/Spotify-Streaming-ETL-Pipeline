import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from config.config import settings
import logging

class SpotifyClient:
    def __init__(self, logger: logging.Logger):
        self.client_id = settings.SPOTIFY_CLIENT_ID
        self.client_secret = settings.SPOTIFY_CLIENT_SECRET
        self.logger = logger
        try:
            client_credentials_manager = SpotifyClientCredentials(
                client_id=self.client_id,
                client_secret=self.client_secret
            )
            self.sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)
            self.logger.info("Spotify client initialized.")
        except Exception as e:
            self.logger.error(f"Error initializing Spotify client: {e}")
            raise

    def get_tracks(self, tracks: list) -> dict:
        """
        Fetches tracks data from Spotify.

        Args:
            tracks (list): A list of track URIs or IDs (max 50).

        Returns:
            dict: JSON response containing track data.
        """
        try:
            response = self.sp.tracks(tracks)
            return response
        except Exception as e:
            self.logger.error(f"Error fetching tracks: {e}")
            raise

    def get_artists(self, artists: list) -> dict:
        """
        Fetches artists data from Spotify.

        Args:
            artists (list): A list of artist URIs or IDs (max 50).

        Returns:
            dict: JSON response containing artists data.
        """

        try:
            response = self.sp.artists(artists)
            return response
        except Exception as e:
            self.logger.error(f"Error fetching artists: {e}")
            raise

    def get_podcasts(self, podcasts: list) -> dict:
        """
        Fetches podcasts data from Spotify.

        Args:
            podcasts (list): A list of podcast URIs or IDs (max 50).

        Returns:
            dict: JSON response containing podcasts data.
        """

        try:
            response = self.sp.shows(podcasts)
            return response
        except Exception as e:
            self.logger.error(f"Error fetching podcasts: {e}")
            raise

    def get_episodes(self, episodes: list) -> dict:
        """
        Fetches episodes data from Spotify.

        Args:
            episodes (list): A list of episode URIs or IDs (max 50).

        Returns:
            dict: JSON response containing episodes data.
        """

        try:
            response = self.sp.episodes(episodes)
            return response
        except Exception as e:
            self.logger.error(f"Error fetching episodes: {e}")
            raise
