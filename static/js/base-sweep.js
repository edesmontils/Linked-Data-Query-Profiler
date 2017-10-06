//<script language="javascript">
// nécessite Prototypejs.org et Script.aculo.us


//=================================================================
// Contrôle de la sortie de page pour éviter de perdre les requêtes
//=================================================================

window.onbeforeunload = function (evt) {
    var message = '';
    if (typeof evt == 'undefined') {
        evt = window.event;
    }
    if (evt) {
        evt.returnValue = message;
    }
    return message;
}

monitor = new Ajax.PeriodicalUpdater('get','/sweep', {
    method: 'get',
    frequency: 1,
    decay: 1.1,
    onSuccess: function (trs) {
            bo = trs.responseText
            $('dashboard').update(bo);
    },
    onFailure: function() {'<p>SWEEP HS</p>' }
    });
monitor.stop();

monitor2 = new Ajax.PeriodicalUpdater('get','/run', {
    method: 'get',
    frequency: 1,
    onSuccess: function (trs) {
            liste = JSON.parse(trs.responseText).result;
            nbBGP = liste[0];
            nbREQ = liste[1];
            //alert(nbBGP);
            // gg1.max = math.max(gg1.max,nbBGP)
            // gg2.max = math.max(gg2.max,nbBGP)
            gg1.refresh(nbBGP,Math.max(gg1.config.max,nbBGP));
            gg2.refresh(nbREQ,Math.max(gg2.config.max,nbREQ));
    },
    onFailure: function() {'<p>SWEEP HS</p>' }
    });

monitor3 = new Ajax.PeriodicalUpdater('get','/pr', {
    method: 'get',
    frequency: 1,
    onSuccess: function (trs) {
            liste = JSON.parse(trs.responseText).result;
            pre = Math.round(liste[0]*100);
            rec = Math.round(liste[1]*100);
            //alert(nbBGP);
            gPRE.refresh(pre);
            gREC.refresh(rec);
    },
    onFailure: function() {'<p>SWEEP HS</p>' }
    });
//=======================================================
// Fonctions de gestion de l'interface et des appels AJAX
//=======================================================

function clear() {	
    // mémorisation des messages et aides pour éviter de charger le serveur.
    messages_aides = null;
    messages = null;
    messages_mentions = null;
    messages_apropos = null;

	// Page effacée
    $('posts').update("");
    $('dashboard').update("");
    $('frequent').update("");
}

function init() {
    clear();

    gg1 = new JustGage({
      id: 'gaugeBGP',
      title: 'Current BGPs\nunder construction',
      min: 0,
      max: 50,
      value: 0,
      donut: true,
      gaugeWidthScale: 0.6,
      counter: true,
      hideInnerShadow: true
      });

    gg2 = new JustGage({
      id: 'gaugeREQ',
      title: 'Current queries to find',
      value: 0,
      min: 0,
      max: 10,
      donut: true,
      gaugeWidthScale: 0.6,
      counter: true,
      hideInnerShadow: true
      });

    gPRE = new JustGage({
        id: 'gaugePRE',
        value: 0,
        min: 0,
        max: 100,
        title: 'Avg Precision',
        gaugeWidthScale: 0.6,
        levelColors: [
                        "#cd0000",
                        "#0066cc",
                        "#006400"
                      ] ,
        symbol: '%',
        pointer: true,
        counter: true,
        humanFriendly: true      });

    gREC = new JustGage({
        id: 'gaugeREC',
        value: 0,
        min: 0,
        max: 100,
        title: 'Avg Recall',
        gaugeWidthScale: 0.6,
        levelColors: [
                        "#cd0000",
                        "#0066cc",
                        "#006400"
                      ] ,
        counter: true,
        symbol: '%',
        pointer: true,
        humanFriendly: true      });

    monitoring();
}

function end() {}

function monitoring() {
    $('frequent').hide()
    $('posts').hide()
    $('dashboard').appear();
    monitor.start()
}

function bestof() {
    monitor.stop()
    $('dashboard').hide()
    $('posts').hide()
    $('frequent').update('<p>Best Of is computing. Please waiting.</p>');
    new Ajax.Request('/bestof', {
        method: 'get',
        onSuccess: function (trs) {
            bo = trs.responseText
            $('frequent').hide();
            $('frequent').update(bo);
            $('frequent').appear();
        },
        onFailure: function () {
            alert('apropos: Unable to produce freuent BGPs and queries !')
        }
    });
}

function aides() {
    monitor.stop()
    $('dashboard').hide();
    $('frequent').hide();
    if (messages_aides == null) {
        messages_aides = '<div class="post"><h2 class="title">Help</h2><div class="story">';
        messages_aides = messages_aides
                +'<p><img src="./static/images/home_64.png" width="32" alt="aide"/> : SWEEP Dashboard.</p>'
                +'<p><img src="./static/images/help_64.png" width="32" alt="aide"/> : this help.</p>'
                +'<p><img src="./static/images/briefcase_64.png" width="32" alt="Best Of !"/> : frequent BGPs and frequent queries.</p>'
                +'<p><img src="./static/images/shield_64.png" width="32" alt="base de données"/> : legal mentions.</p>'
                ;
        messages_aides = messages_aides+ '</div></div>';
        $('posts').update(messages_aides);
        $('posts').appear();        
    } else {
        $('posts').hide();
        $('posts').update(messages_aides);
        $('posts').appear();
    }
}

function mentions() {
    monitor.stop();
    $('dashboard').hide();
    $('frequent').hide();
    if (messages_mentions == null) {
        new Ajax.Request('/mentions', {
            method: 'get',
            onSuccess: function (trs) {
                messages_mentions = trs.responseText
                $('posts').hide();
                $('posts').update(messages_mentions);
                $('posts').appear();
            },
            onFailure: function () {
                alert('mentions: unable to show mentions !')
            }
        });
    } else {
        messages_mentions = '<div class="post"><h2 class="title">Mentions</h2> <h3 class="posted">by E. Desmontils</h3><div class="story">' 
                            + messages_mentions + "</div></div>\n";
        $('posts').hide();
        $('posts').update(messages_mentions);
        $('posts').appear();
    }
}

function apropos() {
    // if (messages_apropos == null) {
    //     new Ajax.Request('/apropos', {
    //         method: 'get',
    //         onSuccess: function (trs) {
    //             messages_apropos = trs.responseText
    //             $('posts').hide();
    //             $('posts').update(messages_apropos);
    //             $('posts').appear();
    //         },
    //         onFailure: function () {
    //             alert('apropos: Impossible d\'obtenir la rubrique !')
    //         }
    //     });
    // } else {
    //     $('posts').hide();
    //     $('posts').update(messages_apropos);
    //     $('posts').appear();
    // }
}
//</script>
