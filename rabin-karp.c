#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include "rabin-karp.h"
#include "sha1.h"

#define SUBSTRING_LEN 5
#define BASE 5
#define MODULO_PRIME 7
#define PATTERN_HASH 4
#define MINCHUNK 8
#define MAXCHUNK 10
#define TRUE 1
#define FALSE 0
#define BITMASK 15

long long int hash_prev=0;
long long int hash_current=0;

int pattern_match(long long int rkhash)
{
	unsigned int num; // Based on the value of MODULO and BASE , the datatype of this has to be changed
	num = (rkhash & BITMASK);
	if(num)
	{
		return TRUE;
	}
	else
	{
		return FALSE;
	}	
}

long int Rabin_Karp_Hash(char substring[],long int start_index,long int end_index)
{
	int i,power;
	if(start_index==0)
	{
		for(i=start_index;i<=end_index;i++)
		{
			power = (int) (pow(BASE,(SUBSTRING_LEN-1-i)));
			hash_current += (((substring[i] % MODULO_PRIME) * ((power)%MODULO_PRIME))%MODULO_PRIME);
		}
	}
	else
	{
		hash_current=((((hash_prev%MODULO_PRIME - ((substring[start_index-1] % MODULO_PRIME) * (((int)pow(BASE,SUBSTRING_LEN-1)) % MODULO_PRIME) % MODULO_PRIME ) % MODULO_PRIME) * (BASE % MODULO_PRIME)) % MODULO_PRIME + substring[start_index+end_index] % MODULO_PRIME) % MODULO_PRIME); 

	}
	hash_prev = hash_current;	
	printf("The Rabin hash string is %s",substring);
	return hash_current;
}

char * copy_substring(char *str, int start,int end)
{
	printf("The string is %s\n", str);
	char *s = (char *)malloc(end-start+2);
	//printf("The string is %s\n", str);
	//char s[MAXCHUNK+1];
	int i=0;
	while(start+i<=end)
	{
		//*s++ = *(str+start+i);
		s[i] = str[start+i];
		//printf("%c,%c",s[i],str[start+i]);
		//start++;
		i++;
	}
	s[i]='\0';
	printf("%s\n",s);
	printf("The file stream after copysubstring loop is %s and size is %d",str,strlen(str));
	return s;

	/*char *to;
	//printf("Block size insaide function is %d - %d", start,end);
  	to = (char *)malloc(end-start+2);
	to=strndup(str+start, end);
	//printf("%s",to);
	return to;
	*/
}


int main(int argc, char *argv[])
{
	FILE *fp;
	//char readfilestring[100000];
	char * readfilestring;
	long long int file_size,file_read,len;
	long long int rkhash;
	long int startblock=0,endblock=0;
	int i=0,j=0,flag=0;
	char *shastring;
	
	// To open each file
	fp = fopen(argv[1],"rb");
	if(fp==NULL)
	{
		printf("File read error");
		return 1;
	}
	
	// To obtain file size
	fseek (fp , 0 , SEEK_END);
	file_size = ftell (fp);
  	rewind (fp);
	
	readfilestring = (char *)malloc(file_size+1); 
	
	if(readfilestring==NULL)
	{
		printf("Memory allocation read in file input string");
	}

	// Copy the file into the string buffer
	file_read = fread(readfilestring,1,file_size,fp);
	if(file_read != file_size)
	{
		printf("File not read into the buffer completely");
		return 1;
	}
	

	printf("%s\n",readfilestring);
	printf("%d",strlen(readfilestring));
	len = strlen(readfilestring)-1;
	
	while(endblock <= len) 
	{
		printf("Loop string is %s\n",readfilestring);
		if(flag==0)
		{
			startblock = endblock;
			i =(startblock + MINCHUNK) - SUBSTRING_LEN;
			endblock=SUBSTRING_LEN-1+i;
			flag++;
		}
		if(endblock >= len)
		{
			printf("Block size is %d to %d\n",startblock,len);
			//shastring = copy_substring(readfilestring,startblock,endblock);
			//printf("%s -  %s\n",shastring,sha1(shastring));
			shastring = copy_substring(readfilestring,startblock,len);
                        printf("%s -  %s\n",shastring,sha1(shastring));
			//printf("End of file\n");
			//free(shastring);
			break;
		} 
		rkhash = Rabin_Karp_Hash(readfilestring,i,endblock);
		//printf("%ld\n",rkhash);
		//printf("The string range is %d - %d\n",i,SUBSTRING_LEN-1+i);
		if(pattern_match(rkhash))
		{
			printf("\nInside if\n");
			printf("Block size is %d to %d\n",startblock,endblock);
			shastring = copy_substring(readfilestring,startblock,endblock);
                        printf("%s - %s\n",shastring,sha1(shastring));
			printf("The readstring is %s\n",readfilestring);
			flag=0;
			//free(shastring);		
		}
		else if((endblock-startblock+1)==MAXCHUNK)
		{
			printf("inside Max chunk block");
			printf("Block size is %d to %d\n",startblock, endblock);
			shastring = copy_substring(readfilestring,startblock,endblock);
                        printf("%s - %s\n",shastring,sha1(shastring));
			printf("The readstring is %s\n",readfilestring);
			flag=0;
			//free(shastring);
		}
		else
		{ 
			//printf("Inside else\n");
		}
		//printf("The i value inside loop%d\n",i);
		i++;
		endblock++;
		//free(shastring);
		//free(shastring);
		//printf("The value of %d - %d - %d",i,endblock,len); 
	}
	//printf("The value of i is %d",i);
	printf("End of while loop");

	

	fclose(fp);
	free(readfilestring);   // Some memory corruption error when this is uncommented 
	return 0;
}
