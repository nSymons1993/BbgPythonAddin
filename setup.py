from distutils.core import setup
setup(
  name = 'BloombergData',
  packages = ['BloombergData'],
  version = '0.2',
  license='MIT',
  description = 'A package to enable retrieval of data through the Bloomberg API into pandas dataframes.',
  author = 'Nicholas Symons',
  author_email = 'n.symons1993@outlook.com',
  url = 'https://github.com/nSymons1993/BbgPythonAddin',
  download_url = 'https://github.com/nSymons1993/BbgPythonAddin/archive/v_02.tar.gz',
  keywords = ['Bloomberg', 'Finance', 'Equities', 'Fixed Income', 'Derivatives', 'Benchmark', 'Index', 'Algorithm', 'Portfolio', 'Trading', 'Management', 'API', 'Financial', 'Machine Learning', 'Statistics', 'FX', 'Options', 'Swaps', 'Swaptions', 'Trade', 'pandas', 'numpy', 'big data'],   # Keywords that define your package best
  install_requires=[
            'blpapi',
            'logging',
            'pandas>=0.24.0',
            'numpy',
            'datetime',
            'pytz',
            'tzlocal'
        ],
  classifiers=[
    'Development Status :: 3 - Alpha',
    'Intended Audience :: Developers',
    'Topic :: Software Development :: Build Tools',
    'License :: OSI Approved :: MIT License',
    'Programming Language :: Python :: 3.6',
    'Programming Language :: Python :: 3.7'
  ],
)