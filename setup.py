from distutils.core import setup
setup(
  name = 'BloombergData',         # How you named your package folder (MyLib)
  packages = ['BloombergData'],   # Chose the same as "name"
  version = '0.1',      # Start with a small number and increase it with every change you make
  license='MIT',        # Chose a license from here: https://help.github.com/articles/licensing-a-repository
  description = 'A package to enable retrieval of data through the Bloomberg API into pandas dataframes.',   # Give a short description about your library
  author = 'Nicholas Symons',                   # Type in your name
  author_email = 'n.symons1993@outlook.com',      # Type in your E-Mail
  url = 'https://github.com/nSymons1993/AladdinScripts',   # Provide either the link to your github or to your website
  download_url = 'https://github.com/nSymons1993/BbgPythonAddin/archive/v_01.tar.gz',    # I explain this later on
  keywords = ['Bloomberg', 'Finance', 'Equities', 'Fixed Income', 'Derivatives', 'Benchmark', 'Index', 'Algorithm', 'Portfolio', 'Trading', 'Management', 'API', 'Financial', 'Machine Learning', 'Statistics', 'FX', 'Options', 'Swaps', 'Swaptions', 'Trade', 'pandas', 'numpy', 'big data'],   # Keywords that define your package best
  install_requires=[            # I get to this in a second
            'blpapi',
            'logging',
            'pandas>=0.24.0',
            'numpy',
            'datetime',
            'pytz',
            'tzlocal'
        ],
  classifiers=[
    'Development Status :: 3 - Alpha',      # Chose either "3 - Alpha", "4 - Beta" or "5 - Production/Stable" as the current state of your package
    'Intended Audience :: Developers',      # Define that your audience are developers
    'Topic :: Software Development :: Build Tools',
    'License :: OSI Approved :: MIT License',   # Again, pick a license
    'Programming Language :: Python :: 3.6',      #Specify which pyhton versions that you want to support
    'Programming Language :: Python :: 3.7'
  ],
)