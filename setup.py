from distutils.core import setup
#from setuptools import setup, find_packages

setup(
    # Application name:
    name="ohmydb",
    
    # Version number (initial):
    version="0.1.0",
    
    # Application author details:
    author="Antoine Honore",
    author_email="ahonore@pm.me",
    
    # Packages
    packages=["patdbvisu"],
    
    # Include additional files into the package
    include_package_data=True,
    
    # Details
    url="http://pypi.python.org/pypi/MyApplication_v010/",
    
    #
    # license="LICENSE.txt",
    description="Useful towel-related stuff.",
    
    # long_description=open("README.txt").read(),
    
    # Dependent packages (distributions)
    install_requires=[
        "et-xmlfile==1.1.0",
"greenlet==1.1.2",
"numpy==1.21.2",
"openpyxl==3.0.9",
"pandas==1.3.4",
"parse==1.19.0",
"psycopg2==2.9.1",
"python-dateutil==2.8.2",
"pytz==2021.3",
"six==1.16.0",
"SQLAlchemy==1.4.26",
"Unidecode==1.3.2",
"plotly~=5.3.1",
"dash~=2.0.0",
 ],
)
