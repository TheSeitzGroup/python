#!/usr/bin/env python
# coding: utf-8

# In[2]:


import numpy as np

a = np.zeros(3) # creates array of zeros (type: floats)
a


# In[3]:


a.shape


# In[12]:


a.shape = (3,1) # changes the shape
a


# In[14]:


z = np.ones(10)
z


# In[23]:


type(z[0])


# In[24]:


y = np.empty(3)
y


# In[26]:


l = np.linspace(2,10, 5) # creates array from 2 to 10 with 5 elements
l


# In[3]:


g = np.array([10, 20]) #ndarray from Python list
g


# In[30]:


list_a = [1,2,3,4,5,6,7]
la = np.array(list_a)
la


# In[31]:


list_b = [[1,2],[3,4]]
lb = np.array([list_b]) # 2dim array
lb 


# In[32]:


lb.shape


# In[33]:


get_ipython().run_line_magic('pinfo', 'z')


# In[34]:


np.random.seed(0)
zl = np.random.randint(10, size=6)
zl


# In[35]:


zl[0] # returns first element of the array


# In[37]:


zl2 = zl.reshape(3,2)
zl2


# In[45]:


zl2[1] # array([3, 3])
zl2[0:2]
zl[0:2] # array([5, 0])
zl[-1] # 9, last element of the 1 dim array


# In[46]:


zl2[::2, ::2]


# In[48]:


from skimage import io
photo = io.imread('1.jpg')


# In[49]:


photo.shape # (598 rows , 477 cols, 3 channels of color RGB)


# In[51]:


import matplotlib.pyplot as plt
plt.imshow(photo)


# In[52]:


plt.imshow(photo[::-1]) # start stop step Reverse the image


# In[54]:


plt.imshow(photo[:, ::-1]) # all rows, reveresed cols, mirror image


# In[55]:


plt.imshow(photo[50:150, 150:280]) # take a section of the photo


# In[56]:


plt.imshow(photo[::2, ::2]) 
# everyother row and everyother col,
# halfed the size of the image


# In[58]:


photo # apply math func to np array
photo_sin = np.sin(photo)
photo_sin # the sin of every element of every element of the np array


# In[64]:


np.sum(photo)
np.prod(photo)
np.mean(photo)
print(np.mean(photo))
print(np.std(photo))
print(np.var(photo))
print(np.min(photo))
print(np.max(photo))
print(np.argmin(photo))
print(np.argmax(photo))


# # Using matrix math to solve systems of equations
# ### 3:37 -- https://www.youtube.com/watch?v=rowWM-MijXU&t=139s&ab_channel=ZachStar
# 
# 3x - 2y = 1    
# -x + 4y = 3

# In[9]:


a = np.array([[3,-2], [-1, 4]])
b = np.array([1,3])
print(a)
print(b)


# In[10]:


s = np.linalg.solve(a, b)
s


# In[11]:


np.allclose(np.dot(a, s), b)


# In[ ]:


# apply an inverse matrix to solve (from youtube video)


# In[ ]:


# Gil strang's linear Alebra book problem in preface page v
# https://ia802906.us.archive.org/18/items/StrangG.LinearAlgebraAndItsApplications45881001/%5BStrang_G.%5D_Linear_algebra_and_its_applications%284%29%5B5881001%5D.pdf


# In[20]:


# c + d = 2
# 2c + 3d = 5
# 3c + 4d = 7

a2 = np.array([[2,3],[3,4]])
b2 = np.array([5,7])
print(a2)
b2


# In[22]:


s2 = np.linalg.solve(a2, b2)
print(s2)
np.allclose(np.dot(a, s2), b)


# In[23]:


np.dot(a,b)


# In[27]:


A3 = np.array([[1,1,2],[2,3,4],[3,4,5]])
B3 = np.array([2,5,7])


# In[29]:


C3 = np.matmul(A3, B3)
C3

