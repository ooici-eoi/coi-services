"""
@file coi-services/ion/idk/driver_generator.py
@author Bill French
@brief Main script class for running the start_driver process
"""

from ion.processes.idk.metadata import Metadata
from ion.processes.idk.comm_config import CommConfig
from ion.processes.idk.driver_generator import DriverGenerator
from ion.processes.idk.comm_config import CommConfig

class StartDriver():
    """
    Main class for running the start driver process.
    """

    def fetch_metadata(self):
        """
        @brief collect metadata from the user
        """
        self.metadata = Metadata()
        self.metadata.get_from_console()

    def fetch_comm_config(self):
        """
        @brief collect connection information for the logger from the user
        """
        self.comm_config = CommConfig.get_config_from_console(self.metadata)
        self.comm_config.get_from_console()

    def generate_code(self):
        """
        @brief generate the directory structure, code and tests for the new driver.
        """
        driver = DriverGenerator( self.metadata )
        driver.generate()

    def run(self):
        """
        @brief Run it.
        """
        print( "*** Starting Driver Creation Process***" )

        self.fetch_metadata()
        self.fetch_comm_config()
        self.generate_code()

if __name__ == '__main__':
    app = StartDriver()
    app.run()