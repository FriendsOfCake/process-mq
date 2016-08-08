# ProcessMQ

[![Build Status](https://img.shields.io/travis/FriendsOfCake/process-mq/master.svg?style=flat-square)](https://travis-ci.org/FriendsOfCake/process-mq)
[![Coverage](https://img.shields.io/coveralls/FriendsOfCake/process-mq/master.svg?style=flat-square)](https://coveralls.io/r/FriendsOfCake/process-mq)
[![Total Downloads](https://img.shields.io/packagist/dt/friendsofcake/process-mq.svg?style=flat-square)](https://packagist.org/packages/friendsofcake/process-mq)
[![License](https://img.shields.io/badge/license-MIT-blue.svg?style=flat-square)](LICENSE)

{{@TODO description}}

## Install

Using [Composer][composer]:

```
composer require friendsofcake/process-mq:dev-master
```

You then need to load the plugin. You can use the shell command:

```
bin/cake plugin load ProcessMQ
```

or by manually adding statement shown below to `bootstrap.php`:

```php
Plugin::load('ProcessMQ');
```

## Usage

{{@TODO documentation}}

## Patches & Features

* Fork
* Mod, fix
* Test - this is important, so it's not unintentionally broken
* Commit - do not mess with license, todo, version, etc. (if you do change any, bump them into commits of
their own that I can ignore when I pull)
* Pull request - bonus point for topic branches

To ensure your PRs are considered for upstream, you MUST follow the [CakePHP coding standards][standards].

## Bugs & Feedback

http://github.com/friendsofcake/process-mq/issues

## License

Copyright (c) 2015, [FriendsOfCake][foc] and licensed under [The MIT License][mit].

[cakephp]:http://cakephp.org
[composer]:http://getcomposer.org
[mit]:http://www.opensource.org/licenses/mit-license.php
[foc]:http://friendsofcake.com
[standards]:http://book.cakephp.org/3.0/en/contributing/cakephp-coding-conventions.html
