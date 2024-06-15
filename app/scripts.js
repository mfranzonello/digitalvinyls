$(document).ready(function () {
    // Fetch albums from the backend
    $.getJSON('/albums', function (data) {
        data.forEach(album => {
            $('#coverflow').append(
                `<img src="${album.image}" alt="${album.name}" data-uri="${album.uri}">`
            );
        });

        // Add click event listener to images
        $('#coverflow img').click(function () {
            const uri = $(this).data('uri');
            $.ajax({
                url: '/play',
                type: 'POST',
                contentType: 'application/json',
                data: JSON.stringify({ uri: uri }),
                success: function (response) {
                    console.log('Playing album:', response);
                }
            });
        });
    });
});