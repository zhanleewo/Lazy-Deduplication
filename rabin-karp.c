#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include "rabin-karp.h"
#include "sha1.h"

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

long int Rabin_Karp_Hash(char substring[],long long int start_index,long long int end_index) 
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
	//printf("The Rabin hash string is %s\n",substring);
	return hash_current;
}

char * copy_substring(char *str, int start,int end)
{
	char *s = (char *)malloc(end-start+2);
	int i=0;
	while(start+i<=end)
	{
		s[i] = str[start+i];
		i++;
	}
	s[i]='\0';
	//printf("The substring is %s\n",s);
	return s;

	/*char *to;
	//printf("Block size insaide function is %d - %d", start,end);
  	to = (char *)malloc(end-start+2);
	to=strndup(str+start, end);
	//printf("%s",to);
	return to;
	*/
}

void create_chunkfile(char str[],char shastr[])
{
	FILE *filech;
	char *filename = (char *)malloc(strlen(shastr)+4);
	strcpy(filename,shastr);
	strcat(filename,".txt");
	printf("Chunk File name - %s\n",filename);
	filech = fopen(filename,"wb");
	if(filech == NULL)
	{
		printf("File chunk not created or returned");
	}
	else
	{
		//printf("Chunk File created");
		fprintf(filech,str);
	}
	free(filename);
	fclose(filech);
}


int main(int argc, char *argv[])
{
	FILE *fp,*metafp;
	char * readfilestring,*metafile;
	long long int file_size,file_read,len;
	long long int rkhash;
	long long int startblock=0,endblock=0;
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

	// Creating the meta file

	metafile = (char *)malloc(strlen(argv[1]+5));
	strcpy(metafile,argv[1]);
	strcat(metafile,".meta");
	metafp = fopen(metafile,"wb");

	printf("The Meta data file - %s\n",metafile);
	//printf("%s\n",readfilestring);
	//printf("%d\n",strlen(readfilestring));
	len = strlen(readfilestring)-1;
	
	while(endblock <= len) 
	{
		if(flag==0)
		{
			startblock = endblock;
			i =(startblock + MINCHUNK) - SUBSTRING_LEN;
			endblock=SUBSTRING_LEN-1+i;
			flag++;
		}
		if(endblock >= len)
		{
			//printf("Block size is %d to %d\n",startblock,len);
			shastring = copy_substring(readfilestring,startblock,len);
                        //printf("%s -  %s\n",shastring,sha1(shastring));
			create_chunkfile(shastring,sha1(shastring));
			fprintf(metafp,sha1(shastring));
			//printf("End of file\n");
			free(shastring);
			break;
		} 
		rkhash = Rabin_Karp_Hash(readfilestring,i,endblock);
		//printf("%ld\n",rkhash);
		if(pattern_match(rkhash))
		{
			//printf("\nInside if\n");
			//printf("Block size is %d to %d\n",startblock,endblock);
			shastring = copy_substring(readfilestring,startblock,endblock);
                        //printf("%s - %s\n",shastring,sha1(shastring));
			create_chunkfile(shastring,sha1(shastring));
			fprintf(metafp,sha1(shastring));
			flag=0;
			free(shastring);		
		}
		else if((endblock-startblock+1)==MAXCHUNK)
		{
			//printf("inside Max chunk block");
			//printf("Block size is %d to %d\n",startblock, endblock);
			shastring = copy_substring(readfilestring,startblock,endblock);
                        //printf("%s - %s\n",shastring,sha1(shastring));
			create_chunkfile(shastring,sha1(shastring));
			fprintf(metafp,sha1(shastring));
			//printf("The readstring is %s\n",readfilestring);
			flag=0;
			free(shastring);
		}
		else
		{ 
			//printf("Inside else\n");
		}
		//printf("The i value inside loop%d\n",i);
		i++;
		endblock++;
		//printf("The value of %d - %d - %d",i,endblock,len); 
	}
	//printf("End of while loop");

	fclose(fp);
	fclose(metafp);
	free(readfilestring);   // Some memory corruption error when this is uncommented 
	return 0;
}
