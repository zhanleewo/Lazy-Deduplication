//Courtesy - SHA1 lib modified from http://ubuntuforums.org/archive/index.php/t-337664.html

#include <gcrypt.h>
#include <stdio.h>
#include <stdlib.h>
#include "sha1.h"

char* sha1(char string[])
{

	/* Length of message to encrypt */
	int msg_len = strlen(string);

	/* Length of resulting sha1 hash - gcry_md_get_algo_dlen
	*returns digest lenght for an algo */
	int hash_len = gcry_md_get_algo_dlen( GCRY_MD_SHA1 );

	/* output sha1 hash - this will be binary data */
	unsigned char hash[ hash_len ];

	/* output sha1 hash - converted to hex representation
	* 2 hex digits for every byte + 1 for trailing \0 */
	char *out = (char *) malloc( sizeof(char) * ((hash_len*2)+2) );
	char *p = out;

	/* calculate the SHA1 digest. This is a bit of a shortcut function
	* most gcrypt operations require the creation of a handle, etc. */
	gcry_md_hash_buffer( GCRY_MD_SHA1, hash, string, msg_len );

	/* Convert each byte to its 2 digit ascii
	* hex representation and place in out */
	int i;
	for ( i = 0; i < hash_len; i++, p += 2 ) 
	{
		snprintf ( p, 3, "%02x", hash[i] );
	}
        out[hash_len*2] = '\n';
        out[hash_len*2+1] = '\0';
	return out;
}
