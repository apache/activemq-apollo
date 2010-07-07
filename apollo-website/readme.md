# Project Website

## Overview

This source module generates the project websites. It uses
[webgen][] for to generate a static website from
plain text files written in a wiki like notation.

[webgen]:http://webgen.rubyforge.org/

## Installing webgen

You first need [ruby][] and [rubygems][] installed on your system. You
should have gems version of 1.3.4 or greater. You can check this via

gem --version

Then you can install webgen and with all the optional features that this
website project uses with the following command:

    sudo gem install webgen coderay feedtools haml RedCloth

[ruby]:http://www.ruby-lang.org/en/downloads/
[rubygems]:http://rubygems.org/pages/download

## Building the Website

It's as simple as running `webgen` in the current directory. The website
will be generated to the out directory.





