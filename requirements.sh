# upgrade all brew packages & update homebrew
brew upgrade && brew update

# install wget to download local copies of files via the Internet
brew install wget

# install a suite of command-line tools for working with CSV, the king of tabular file formats
brew install csvkit

# install open source implementation of the Java Platform, Standard Edition
brew cask install adoptopenjdk

# install the scala programming language
brew install scala

# install java8
brew cask install homebrew/cask-versions/adoptopenjdk8

# install apache spark
brew install apache-spark

# install hadoop
# note: in case you wish to use other types of clusters
brew install hadoop
