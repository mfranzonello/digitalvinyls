/*
 * This is an example of a basic node.js script that performs
 * the Authorization Code oAuth2 flow to authenticate against
 * the Spotify Accounts.
 *
 * For more information, read
 * https://developer.spotify.com/web-api/authorization-guide/#authorization_code_flow
 */


/*
 * Make sure to have Node.js installed
 * Navigate to directory
 * npm install dotenv express request cors cookie-parser
 * node authorize_spotify.js
 * Open localhost:8888 and copy the refresh token
 */

require('dotenv').config({path: '../../.env'});

var express = require('express'); // Express web server framework
var request = require('request'); // "Request" library
var cors = require('cors');
var querystring = require('querystring');
var cookieParser = require('cookie-parser');
var fs = require('fs'); // for JSON output

var client_id = process.env.SPOTIFY_CLIENT_ID; // Your client id
var client_secret = process.env.SPOTIFY_CLIENT_SECRET; // Your secret
var redirect_uri = 'http://localhost:8888/callback'; // Your redirect uri -> move to structure file

/*
 * Generates a random string containing numbers and letters
 * @param  {number} length The length of the string
 * @return {string} The generated string
 */
var generateRandomString = function(length) {
    var text = '';
    var possible = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';

      for (var i = 0; i < length; i++) {
        text += possible.charAt(Math.floor(Math.random() * possible.length));
      }
    return text;
};

var stateKey = 'spotify_auth_state';

var app = express();

var scope = ''

// authorization code
app.use(express.static(__dirname + '/public'))
    .use(cors())
    .use(cookieParser());

app.get('/login', function(req, res) {
    var state = generateRandomString(16);
    res.cookie(stateKey, state);

    // your application requests authorization
    //var scope = ''
    fs.readFile('scopes.json', (error, data) => {
        // if the reading process failed,
        // throwing the error
        if (error) {
            // logging the error
            console.error(error);
            throw err;
        }

        // parsing the JSON object
        // to convert it to a JavaScript object
        var scopes = JSON.parse(data);
        scope = scopes.join(' ')

        // printing the JavaScript object
        // retrieved from the JSON file
        console.log(scopes);
    })

    //var scope = 'playlist-modify-public ugc-image-upload user-read-private user-read-email user-library-read';
    res.redirect('https://accounts.spotify.com/authorize?' +
        querystring.stringify({
            response_type: 'code',
            client_id: client_id,
            scope: scope,
            redirect_uri: redirect_uri,
            state: state
        })
    );
});

app.get('/callback', function(req, res) {
    // your application requests refresh and access tokens
    // after checking the state parameter

    var code = req.query.code || null;
    var state = req.query.state || null;
    var storedState = req.cookies ? req.cookies[stateKey] : null;

    if (state === null || state !== storedState) {
        res.redirect('/#' +
            querystring.stringify({
                error: 'state_mismatch'
            })
        );
    }
    else {
        res.clearCookie(stateKey);

        var authOptions = {
            url: 'https://accounts.spotify.com/api/token',
            form: {
                code: code,
                redirect_uri: redirect_uri,
                grant_type: 'authorization_code'
            },
            headers: {
                'Authorization': 'Basic ' + (new Buffer(client_id + ':' + client_secret).toString('base64'))
            },
            json: true
        };

        request.post(authOptions, function(error, response, body) {
            if (!error && response.statusCode === 200) {

                var access_token = body.access_token,
                    refresh_token = body.refresh_token;

                var options = {
                    url: 'https://api.spotify.com/v1/me',
                    headers: { 'Authorization': 'Bearer ' + access_token },
                    json: true
                };

            // use the access token to access the Spotify Web API
            request.get(options, function(error, response, body) {
                console.log(body);
                
                var display_name = body.display_name,
                    user_id = body.id,
                    imgs = body.images[1].url;

                // output JSON
                // initializing a JavaScript object
                var spot_user = {
                    display_name: display_name,
                    refresh_token: refresh_token,
                    scope: scope,
                    img_src: imgs,
                    //access_token: access_token,
                };

                // converting the JSON object to a string
                var spot_data = JSON.stringify(spot_user);

                // writing the JSON string content to a file
                var fn = 'tokens/' + user_id + ".json";

                fs.writeFile(fn, spot_data, (error) => {
                    // throwing the error
                    // in case of a writing problem
                    if (error) {
                        // logging the error
                        console.error(error);

                        throw error;
                    }

                    console.log("spot_data.json written correctly");
                });
            });

            // print output
            //console.log("Access token: " + access_token)
	        console.log("Refresh token: " + refresh_token)

            // we can also pass the token to the browser to make requests from there
            res.redirect('/#' +
                querystring.stringify({
                    access_token: access_token,
                    refresh_token: refresh_token
                })
            );
        }
        
        else {
            res.redirect('/#' +
                querystring.stringify({
                    error: 'invalid_token'
                })
            );
        }
    });
    }
});

app.get('/refresh_token', function(req, res) {
    // requesting access token from refresh token
    var refresh_token = req.query.refresh_token;
    var authOptions = {
        url: 'https://accounts.spotify.com/api/token',
        headers: { 'Authorization': 'Basic ' + (new Buffer(client_id + ':' + client_secret).toString('base64')) },
        form: {
            grant_type: 'refresh_token',
            refresh_token: refresh_token
        },
        json: true
    };

    request.post(authOptions, function(error, response, body) {
        if (!error && response.statusCode === 200) {
            var access_token = body.access_token;
            res.send({
            'access_token': access_token
            });
        }
    });
});

console.log('Listening on 8888');
app.listen(8888);